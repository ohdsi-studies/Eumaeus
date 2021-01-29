# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of Eumaeus
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

analyseResults <- function(outputFolder) {
  resultsFolder <- file.path(outputFolder , "results")
  if (!file.exists(resultsFolder)) {
    dir.create(resultsFolder)
  }
  
  method <- "HistComp"
  estimates <- loadEstimates(file.path(outputFolder, "hcSummary.csv"))
  analysisDesc <- readr::read_csv(file.path(outputFolder, "hcAnalysisDesc.csv"), col_types = readr::cols())
  analyseMethodResults(method = method,
                       estimates = estimates, 
                       analysisDesc = analysisDesc,
                       aucVariable = "llr",
                       resultsFolder = resultsFolder,
                       maxCores = maxCores)
  
  method <- "CohortMethod"
  estimates <- loadEstimates(file.path(outputFolder, "cmSummary.csv"))
  analysisDesc <- readr::read_csv(file.path(outputFolder, "cmAnalysisDesc.csv"), col_types = readr::cols())
  analyseMethodResults(method = method,
                       estimates = estimates, 
                       analysisDesc = analysisDesc,
                       aucVariable = "logRr",
                       resultsFolder = resultsFolder,
                       maxCores = maxCores)
  
  
}

analyseMethodResults <- function(method,
                                 estimates,
                                 analysisDesc,
                                 aucVariable,
                                 resultsFolder,
                                 maxCores) {
  allControls <- loadAllControls(outputFolder)
  exposures <- loadExposuresofInterest()
  
  estimates <-   select(allControls, .data$exposureId, .data$outcomeId, .data$targetEffectSize, .data$trueEffectSize) %>%
    left_join(estimates, by = c("exposureId", "outcomeId")) %>%
    inner_join(select(exposures, .data$exposureId, .data$exposureName), by = "exposureId")
  
  # analysisId <- 2
  analyseAnalysis <- function(analysisId) {
    analysisSubset <- estimates %>%
      filter(.data$analysisId == !!analysisId)
    description <- analysisDesc %>%
      filter(.data$analysisId == !!analysisId) %>%
      pull(.data$description)
    
    
    # subset <- split(analysisSubset, analysisSubset$exposureId)[[1]]
    analyseExposureAnalysis <- function(subset) {
      exposureId <- subset$exposureId[1]
      exposureName <- subset$exposureName[1]
      title <-  sprintf("%s, Analysis %d (%s)", exposureName, analysisId, description)
      if ("llr" %in% colnames(subset)) {
        # fileName <- file.path(resultsFolder, sprintf("llr_m%s_e%s_a%s_time.png", method, exposureId, analysisId))
        # plotLrr(subset = subset, title = title, scale = "time", fileName = fileName)
        # 
        # fileName <- file.path(resultsFolder, sprintf("llr_m%s_e%s_a%s_events.png", method, exposureId, analysisId))
        # plotLrr(subset = subset, title = title, scale = "events", fileName = fileName)
        
        fileName <- file.path(resultsFolder, sprintf("sensSpec_m%s_e%s_a%s.png", method, exposureId, analysisId))
        plotSensSpec(subset = subset, title = title, fileName = fileName, resultsFolder = resultsFolder, maxCores = maxCores)
        
      }
      # fileName <- file.path(resultsFolder, sprintf("auc_m%s_e%s_a%s.png", method, exposureId, analysisId))
      # plotAuc(subset = subset, title = title, aucVariable = aucVariable, fileName = fileName)
      # 
      # fileName <- file.path(resultsFolder, sprintf("auc_m%s_e%s_a%s_llr.png", method, exposureId, analysisId))
      # plotAuc(subset = subset, title = title, aucVariable = "llr", fileName = fileName)
      
      fileName <- file.path(resultsFolder, sprintf("bias_m%s_e%s_a%s.png", method, exposureId, analysisId))
      plotBias(subset = subset, title = title, fileName = fileName)
      
      # lastSeqId <- max(subset$seqId)
      # lastPeriod <- subset$period[subset$seqId == lastSeqId][1]
      fileName <- file.path(resultsFolder, sprintf("ncs_m%s_e%s_a%s.png", method, exposureId, analysisId))
      plotNcs(subset = subset, title = title, fileName = fileName)
    }
    purrr::map(split(analysisSubset, analysisSubset$exposureId), analyseExposureAnalysis)
  }
  purrr::map(unique(estimates$analysisId), analyseAnalysis)
}

computeCv <- function(sampleSize, alpha) {
  return(Sequential::CV.Poisson(SampleSize = sampleSize,
                                D = 0,
                                M = 1,
                                alpha = alpha))
}

computeThreshold <- function(alpha = 0.01, beta = 0.20, expectedOutcomes = NULL, resultsFolder = NULL, maxCores = NULL) {
  if (is.null(expectedOutcomes)) {
    return(log((1-beta) / alpha))
  } else {
    cacheFile <- file.path(resultsFolder, "criticalValuesCache.rds")
    if (file.exists(cacheFile)) {
      cache <- readRDS(cacheFile)
    } else {
      cache <- tibble(sampleSize = 1.0,
                      critivalValue = computeCv(1, alpha))
    }
    expectedOutcomes[!is.na(expectedOutcomes) & expectedOutcomes > 100] <- 100
    expectedOutcomes[!is.na(expectedOutcomes) & expectedOutcomes < 0.011] <- 0.011
    uniqueEos <- unique(expectedOutcomes)
    idx <- match(uniqueEos, cache$sampleSize) 
    criticalValues <- cache$critivalValue[idx]
    if (any(is.na(idx))) {
      cluster <- ParallelLogger::makeCluster(maxCores)
      result <- ParallelLogger::clusterApply(cluster, uniqueEos[is.na(idx)], computeCv, alpha = alpha)
      criticalValues[is.na(idx)] <- unlist(result)
      ParallelLogger::stopCluster(cluster)
      cache <- bind_rows(cache,
                         tibble(sampleSize = uniqueEos[is.na(idx)],
                                critivalValue = criticalValues[is.na(idx)]))
      saveRDS(cache, cacheFile)                   
    }
    return(criticalValues[match(expectedOutcomes, uniqueEos)])
  }
}

plotLrr <- function(subset, title, scale = "time", fileName) {
  periods <- subset %>%
    distinct(.data$seqId, .data$period)
  
  subset <- subset %>%
    mutate(label = ifelse(.data$trueEffectSize == 1, "Negative control", "Positive control"))
  
  threshold <- computeThreshold(alpha = 0.01, beta = 0.20)
  
  if (scale == "time") {
    plot <- ggplot2::ggplot(subset, ggplot2::aes(x = .data$seqId, y = .data$llr, color = .data$label, group = .data$outcomeId)) +
      ggplot2::geom_line(alpha = 0.8) +
      ggplot2::geom_hline(yintercept = threshold, linetype = "dashed") +
      ggplot2::scale_x_continuous("Accumulated time", breaks = periods$seqId, labels = periods$period) +
      ggplot2::scale_y_continuous("LLR") +
      ggplot2::scale_color_manual(values = c(rgb(0, 0, 0.8), rgb(0.8, 0, 0))) +
      ggplot2::coord_cartesian(ylim = c(0, 10)) +
      ggplot2::ggtitle(title) +
      ggplot2::theme(legend.title = ggplot2::element_blank())
  } else {
    if ("eventsTarget" %in% colnames(subset)) {
      subset$targetOutcomes <- subset$eventsTarget
    }
    plot <- ggplot2::ggplot(subset, ggplot2::aes(x = .data$targetOutcomes, y = .data$llr, color = .data$label, group = .data$outcomeId)) +
      ggplot2::geom_line(alpha = 0.8) +
      ggplot2::geom_hline(yintercept = threshold, linetype = "dashed") +
      ggplot2::scale_x_continuous("Exposed events") +
      ggplot2::scale_y_continuous("LLR") +
      ggplot2::scale_color_manual(values = c(rgb(0, 0, 0.8), rgb(0.8, 0, 0))) +
      ggplot2::coord_cartesian(ylim = c(0, 10), xlim = c(0, 100)) +
      ggplot2::ggtitle(title) +
      ggplot2::theme(legend.title = ggplot2::element_blank())
  }
  ggplot2::ggsave(filename = fileName, plot = plot, width = 10, height = 10, dpi = 300)
}

plotSensSpec <- function(subset = subset, title = title, fileName = fileName, resultsFolder, maxCores) {
  periods <- subset %>%
    distinct(.data$seqId, .data$period)
  
  subset$threshold <- computeThreshold(alpha = 0.01, expectedOutcomes = subset$expectedOutcomes, resultsFolder = resultsFolder, maxCores = maxCores)
  
  counts <- subset %>%
    mutate(groundTruth = .data$trueEffectSize > 1,
           system = !is.na(.data$llr) & .data$llr > .data$threshold) %>%
    group_by(.data$groundTruth, .data$system, .data$seqId) %>%
    summarise(count = n(), .groups = "drop")
  
  # confusionMatrix <- split(counts, counts$seqId)[[1]]
  computeSensSpec <- function(confusionMatrix) {
    tp <- sum(confusionMatrix$count[confusionMatrix$groundTruth == 1 & confusionMatrix$system == 1])
    fp <- sum(confusionMatrix$count[confusionMatrix$groundTruth == 0 & confusionMatrix$system == 1])
    tn <- sum(confusionMatrix$count[confusionMatrix$groundTruth == 0 & confusionMatrix$system == 0])
    fn <- sum(confusionMatrix$count[confusionMatrix$groundTruth == 1 & confusionMatrix$system == 0])
    sensitivity = tp / (tp + fn)
    specificity = tn / (fp + tn)
    return(tibble(seqId = rep(confusionMatrix$seqId[1], 2),
                  metric = c("Sensitivity", "Specificity"),
                  value = c(sensitivity, specificity)))
  }
  results <- purrr::map_dfr(split(counts, counts$seqId), computeSensSpec)
  nominal <- tibble(x = rep(1, 2),
                    y = c(0.99, 0.80),
                    metric = c("Specificity", "Sensitivity"),
                    label = c("Nominal specificity", "Nominal sensitivity"))
  
  plot <- ggplot2::ggplot(results, ggplot2::aes(x = .data$seqId, y = .data$value, color = .data$metric, group = .data$metric)) +
    ggplot2::geom_line(alpha = 0.8) +
    ggplot2::geom_hline(ggplot2::aes(yintercept = y, color = .data$metric), data = nominal, linetype = "dashed") +
    ggplot2::geom_label(ggplot2::aes(y = .data$y, x = .data$x, label = .data$label), data = nominal, hjust = 0, color = "black", fill = rgb(1, 1, 1, alpha = 0.9)) +
    ggplot2::scale_x_continuous("Accumulated time", breaks = periods$seqId, labels = periods$period) +
    ggplot2::scale_y_continuous("Sensitivity or specificity") +
    ggplot2::scale_color_manual(values = c(rgb(0, 0, 0.8), rgb(0.8, 0, 0))) +
    ggplot2::coord_cartesian(ylim = c(0, 1)) +
    ggplot2::ggtitle(title) +
    ggplot2::theme(legend.title = ggplot2::element_blank())
  ggplot2::ggsave(filename = fileName, plot = plot, width = 7, height = 5, dpi = 300)
}

plotAuc <- function(subset, title, aucVariable, fileName) {
  
  ncs <- subset %>%
    filter(.data$targetEffectSize == 1) %>%
    mutate(treatment = 0)
  
  pcs <- subset %>%
    filter(.data$targetEffectSize != 1) %>%
    mutate(treatment = 1)
  
  # data <- split(pcs, paste(pcs$seqId, pcs$targetEffectSize))[[20]]
  computeAuc <- function(data) {
    data <- bind_rows(data, filter(ncs, .data$seqId == data$seqId[1]))
    data$propensityScore <- pull(data, aucVariable)
    data$propensityScore[is.na(data$propensityScore)] <- 0
    aucs <- CohortMethod::computePsAuc(data, confidenceIntervals = TRUE)
    return(tibble(auc = aucs$auc,
                  aucLb95ci = aucs$aucLb95ci,
                  aucUb95ci = aucs$aucUb95ci,
                  seqId = data$seqId[1],
                  effectSize = max(data$targetEffectSize)))
  }
  aucs <- purrr::map_dfr(split(pcs, paste(pcs$seqId, pcs$targetEffectSize)), computeAuc)
  aucs$effectSize <- as.factor(aucs$effectSize)
  colnames(aucs)[colnames(aucs) == "effectSize"] <- "True effect size"
  periods <- subset %>%
    distinct(.data$seqId, .data$period)
  
  plot <- ggplot2::ggplot(aucs, ggplot2::aes(x = .data$seqId, 
                                             y = .data$auc, 
                                             ymin = .data$aucLb95ci, 
                                             ymax = .data$aucUb95ci, 
                                             color = .data$`True effect size`,
                                             fill = .data$`True effect size`, 
                                             group = .data$`True effect size`)) +
    ggplot2::geom_ribbon(alpha = 0.2, color = rgb(0, 0, 0, alpha = 0)) +
    ggplot2::geom_line(alpha = 0.8) +
    ggplot2::scale_x_continuous("Accumulated time", breaks = periods$seqId, labels = periods$period) +
    ggplot2::scale_y_continuous("AUC") +
    ggplot2::coord_cartesian(ylim = c(0.5, 1)) +
    ggplot2::ggtitle(title) 
  ggplot2::ggsave(filename = fileName, plot = plot, width = 10, height = 10, dpi = 300)
}

plotBias <- function(subset = subset, title = title, fileName = fileName) {
  ncs <- subset %>%
    filter(.data$targetEffectSize == 1 & .data$expectedOutcomes > 1) %>%
    mutate(treatment = 0) %>%
    arrange(.data$seqId)
  
  plot <- EvidenceSynthesis::plotEmpiricalNulls(logRr = ncs$logRr,
                                                seLogRr = ncs$seLogRr,
                                                labels = ncs$period,
                                                showCis = F)
  
  ggplot2::ggsave(fileName,
                  plot,
                  width = 10,
                  height = 1 + length(unique(ncs$period)) * 0.2,
                  dpi = 400)
}

plotNcs <- function(subset = subset, title = title, fileName = fileName) {
  ncs <- subset %>%
    filter(.data$targetEffectSize == 1 & .data$expectedOutcomes > 1) %>%
    mutate(treatment = 0) %>%
    arrange(.data$seqId)
  
  lastNcs <- ncs %>%
    filter(.data$seqId == max(ncs$seqId))
  
  EmpiricalCalibration::plotCalibrationEffect(logRrNegatives = lastNcs$logRr,
                                              seLogRrNegatives = lastNcs$seLogRr,
                                              showCis = TRUE,
                                              title = title,
                                              fileName = fileName)
  # ncs$rr <- exp(ncs$logRr)
  # ggplot2::ggplot(ncs, ggplot2::aes(x = .data$seqId, y = .data$rr, ymin = .data$lb95Ci, ymax = .data$ub95Ci, group = .data$outcomeId)) +
  #   ggplot2::geom_line(color = rgb(0, 0, 0.8, alpha = 0.05)) +
  #   ggplot2::geom_ribbon(color = rgb(0, 0, 0, alpha = 0), fill = rgb(0, 0, 0.8, alpha = 0.05)) +
  #   ggplot2::scale_y_log10()
  
}