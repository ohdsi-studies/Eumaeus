# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of VaccineSurveillanceMethodEvaluation
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
  
  allControls <- loadAllControls(outputFolder)
  estimates <- estimates %>%
    inner_join(select(allControls, .data$exposureId, .data$outcomeId, .data$targetEffectSize, .data$trueEffectSize), by = c("exposureId", "outcomeId"))
  
  # analysisId <- 1
  analyseAnalysis <- function(analysisId) {
    subset <- estimates %>%
      filter(.data$analysisId == !!analysisId)
    description <- analysisDesc %>%
      filter(.data$analysisId == !!analysisId) %>%
      pull(.data$description)
    exposureId <- subset$exposureId[1]
    title <-  sprintf("Analysis %d: %s", analysisId, description)
    
    fileName <- file.path(resultsFolder, sprintf("llr_e%s_a%s_time.png", exposureId, analysisId))
    plotLrr(subset = subset, title = title, scale = "time", fileName = fileName)
    
    fileName <- file.path(resultsFolder, sprintf("llr_e%s_a%s_events.png", exposureId, analysisId))
    plotLrr(subset = subset, title = title, scale = "events", fileName = fileName)
  }
  purrr::map(unique(estimates$analysisId), analyseAnalysis)
}

computeThreshold <- function(alpha = 0.01, beta = 0.20) {
  return(log((1-beta) / alpha))
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
