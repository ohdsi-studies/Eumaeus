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

exportResults <- function(outputFolder,
                          connectionDetails,
                          cdmDatabaseSchema,
                          databaseId,
                          databaseName,
                          databaseDescription,
                          minCellCount = 5,
                          maxCores) {
  exportFolder <- file.path(outputFolder, "export")
  if (!file.exists(exportFolder)) {
    dir.create(exportFolder, recursive = TRUE)
  }
  
  exportAnalyses(outputFolder = outputFolder,
                 exportFolder = exportFolder)
  
  exportExposures(outputFolder = outputFolder,
                  exportFolder = exportFolder)
  
  exportOutcomes(outputFolder = outputFolder,
                 exportFolder = exportFolder)
  
  exportMetadata(outputFolder = outputFolder,
                 exportFolder = exportFolder,
                 connectionDetails = connectionDetails,
                 cdmDatabaseSchema = cdmDatabaseSchema,
                 databaseId = databaseId,
                 databaseName = databaseName,
                 databaseDescription = databaseDescription,
                 minCellCount = minCellCount)
  
  exportMainResults(outputFolder = outputFolder,
                    exportFolder = exportFolder,
                    databaseId = databaseId,
                    minCellCount = minCellCount,
                    maxCores = maxCores)
  
  # exportDiagnostics(outputFolder = outputFolder,
  #                   exportFolder = exportFolder,
  #                   databaseId = databaseId,
  #                   minCellCount = minCellCount,
  #                   maxCores = maxCores)
  
  # Add all to zip file -------------------------------------------------------------------------------
  ParallelLogger::logInfo("Adding results to zip file")
  zipName <- normalizePath(file.path(exportFolder, sprintf("Results_%s.zip", databaseId)), mustWork = FALSE)
  files <- list.files(exportFolder, pattern = ".*\\.csv$")
  oldWd <- setwd(exportFolder)
  on.exit(setwd(oldWd))
  DatabaseConnector::createZipFile(zipFile = zipName, files = files)
  ParallelLogger::logInfo("Results are ready for sharing at:", zipName)
}

exportAnalyses <- function(outputFolder, exportFolder) {
  ParallelLogger::logInfo("Exporting analyses")
  ParallelLogger::logInfo("- analysis table")
  
  pathToCsv <- system.file("settings", "Analyses.csv", package = "Eumaeus")
  analysis <- readr::read_csv(pathToCsv, col_types = readr::cols())
  colnames(analysis) <- SqlRender::camelCaseToSnakeCase(colnames(analysis))
  fileName <- file.path(exportFolder, "analysis.csv")
  readr::write_csv(analysis, fileName)
  
  ParallelLogger::logInfo("- time_period table")
  eois <- loadExposuresofInterest()
  generateTimePeriods <- function(row) {
    splitTimePeriod(row$startDate, row$endDate) %>%
      mutate(exposureId = row$exposureId) %>%
      rename(periodId = .data$seqId)
  }
  timePeriod <- purrr::map_dfr(split(eois, 1:nrow(eois)), generateTimePeriods)
  colnames(timePeriod) <- SqlRender::camelCaseToSnakeCase(colnames(timePeriod))
  fileName <- file.path(exportFolder, "time_period.csv")
  readr::write_csv(timePeriod, fileName)
}

exportExposures <- function(outputFolder, exportFolder) {
  ParallelLogger::logInfo("Exporting exposures")
  ParallelLogger::logInfo("- exposure table")
  exposure <- loadExposureCohorts(outputFolder) %>%
    filter(.data$sampled == FALSE & .data$comparator == FALSE) %>%
    select(.data$exposureId, .data$exposureName, totalShots = .data$shots, .data$baseExposureId, .data$baseExposureName, .data$shot)
  colnames(exposure) <- SqlRender::camelCaseToSnakeCase(colnames(exposure))
  fileName <- file.path(exportFolder, "exposure.csv")
  readr::write_csv(exposure, fileName)
}

exportOutcomes <- function(outputFolder, exportFolder) {
  ParallelLogger::logInfo("Exporting outcomes")
  
  ParallelLogger::logInfo("- negative_control_outcome table")
  negativeControl <- loadNegativeControls()
  colnames(negativeControl) <- SqlRender::camelCaseToSnakeCase(colnames(negativeControl))
  fileName <- file.path(exportFolder, "negative_control_outcome.csv")
  readr::write_csv(negativeControl, fileName)
  
  positiveControl <- loadAllControls(outputFolder) %>%
    filter(.data$targetEffectSize > 1) %>%
    transmute(outcomeId = .data$outcomeId,
              outcomeName = .data$outcomeName,
              exposureId = .data$exposureId,
              negativeControlId = .data$oldOutcomeId,
              effectSize = .data$targetEffectSize)
  colnames(positiveControl) <- SqlRender::camelCaseToSnakeCase(colnames(positiveControl))
  fileName <- file.path(exportFolder, "positive_control_outcome.csv")
  readr::write_csv(positiveControl, fileName)
}

exportMetadata <- function(outputFolder,
                           exportFolder,
                           connectionDetails,
                           cdmDatabaseSchema,
                           databaseId,
                           databaseName,
                           databaseDescription,
                           minCellCount) {
  ParallelLogger::logInfo("Exporting metadata")
  
  ParallelLogger::logInfo("- database table")
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  observationPeriodRange <- getObservationPeriodDateRange(connection = connection,
                                                          cdmDatabaseSchema = cdmDatabaseSchema)
  vocabularyVersion <- getVocabularyVersion(connection = connection,
                                            cdmDatabaseSchema = cdmDatabaseSchema)
  
  database <- tibble::tibble(databaseId = databaseId,
                             databaseName = databaseName,
                             description = databaseDescription,
                             vocabularyVersion = vocabularyVersion,
                             minObsPeriodDate = observationPeriodRange$minDate,
                             maxObsPeriodDate = observationPeriodRange$maxDate,
                             studyPackageVersion = utils::packageVersion("Eumaeus"),
                             isMetaAnalysis = 0)
  colnames(database) <- SqlRender::camelCaseToSnakeCase(colnames(database))
  fileName <- file.path(exportFolder, "database.csv")
  readr::write_csv(database, fileName)
  
  ParallelLogger::logInfo("- database_characterization table")
  pathToCsv <- file.path(outputFolder, "DbCharacterization.csv")
  table <- readr::read_csv(pathToCsv, col_types = readr::cols())  
  table$databaseId <- databaseId
  table <- enforceMinCellValue(table, "count", minValues = minCellCount)
  colnames(table) <- SqlRender::camelCaseToSnakeCase(colnames(table))
  fileName <- file.path(exportFolder, "database_characterization.csv")
  readr::write_csv(table, fileName)
}

getVocabularyVersion <- function(connection, cdmDatabaseSchema) {
  sql <- "SELECT vocabulary_version FROM @cdm_database_schema.vocabulary WHERE vocabulary_id = 'None';"
  vocabularyVersion <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                                  sql = sql,
                                                                  cdm_database_schema = cdmDatabaseSchema,
                                                                  snakeCaseToCamelCase = TRUE)[1, 1]
  return(vocabularyVersion)
}

getObservationPeriodDateRange <- function(connection, cdmDatabaseSchema) {
  sql <- "SELECT MIN(observation_period_start_date) min_date, MAX(observation_period_end_date) max_date FROM @cdm_database_schema.observation_period;"
  observationPeriodDateRange <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                                           sql = sql,
                                                                           cdm_database_schema = cdmDatabaseSchema,
                                                                           snakeCaseToCamelCase = TRUE)
  return(observationPeriodDateRange)
}

enforceMinCellValue <- function(data, fieldName, minValues, silent = FALSE) {
  toCensor <- !is.na(pull(data, fieldName)) & pull(data, fieldName) < minValues & pull(data, fieldName) != 0
  if (!silent) {
    percent <- round(100 * sum(toCensor)/nrow(data), 1)
    ParallelLogger::logInfo("   censoring ",
                            sum(toCensor),
                            " values (",
                            percent,
                            "%) from ",
                            fieldName,
                            " because value below minimum")
  }
  if (length(minValues) == 1) {
    data[toCensor, fieldName] <- -minValues
  } else {
    data[toCensor, fieldName] <- -minValues[toCensor]
  }
  return(data)
}

exportMainResults <- function(outputFolder,
                              exportFolder,
                              databaseId,
                              minCellCount,
                              maxCores) {
  ParallelLogger::logInfo("Exporting main results")
  
  ParallelLogger::logInfo("- estimate table")
  columns <- c("databaseId",  "method", "analysisId", "exposureId", "outcomeId", "periodId", "rr", "ci95lb", "ci95ub", "p", "exposureSubjects", "counterfactualSubjects", "exposureDays", "counterfactualDays", "exposureOutcomes", "counterfactualOutcomes", "logRr", "seLogRr", "llr", "criticalValue")
  
  ParallelLogger::logInfo("  - Adding cohort method estimates")
  historicComparatorEstimates <- loadEstimates(file.path(outputFolder, "hcSummary_withCvs.csv")) %>%
    mutate(databaseId = !!databaseId,
           method = "HistoricalComparator",
           periodId = .data$seqId,
           exposureSubjects = .data$targetSubjects,
           exposureOutcomes = .data$targetOutcomes,
           exposureDays = .data$targetYears * 365.25,
           counterfactualSubjects = .data$comparatorSubjects,
           counterfactualOutcomes = .data$comparatorOutcomes,
           counterfactualDays = .data$comparatorYears * 365.25,
           rr = .data$irr,
           ci95lb = .data$lb95Ci,
           ci95ub = .data$ub95Ci,
           p = 2 * pmin(pnorm(.data$logRr/.data$seLogRr), 1 - pnorm(.data$logRr/.data$seLogRr))) %>%
    select(all_of(columns))

  ParallelLogger::logInfo("  - Adding cohort method estimates")
  exposures <- loadExposureCohorts(outputFolder) 
  mapping <- exposures %>%
    filter(.data$sampled == FALSE) %>%
    select(nonSampleExposureId = .data$exposureId, .data$baseExposureId, .data$shot, .data$comparator) %>%
    inner_join(exposures %>%
                 filter(.data$sampled == TRUE), 
               by = c("baseExposureId", "shot", "comparator")) %>%
    select(.data$nonSampleExposureId, .data$exposureId) 
  
  cohortMethodEstimates <- loadEstimates(file.path(outputFolder, "cmSummary_withCvs.csv")) %>%
    inner_join(mapping, by = "exposureId") %>%
    mutate(databaseId = !!databaseId,
           method = "CohortMethod",
           periodId = .data$seqId,
           exposureId = .data$nonSampleExposureId,
           exposureSubjects = .data$target,
           exposureOutcomes = .data$eventsTarget,
           exposureDays = .data$targetDays,
           counterfactualSubjects = .data$comparator,
           counterfactualOutcomes = .data$eventsComparator,
           counterfactualDays = .data$comparatorDays,
           llr = if_else(!is.na(.data$logRr) & .data$logRr < 0, 0, .data$llr)) %>%
    select(all_of(columns))

  ParallelLogger::logInfo("  - Adding case-control estimates")
  caseControlEstimates <- loadEstimates(file.path(outputFolder, "ccSummary_withCvs.csv")) %>%
    mutate(databaseId = !!databaseId,
           method = "CaseControl",
           periodId = .data$seqId,
           exposureSubjects = .data$exposedCases + .data$exposedControls,
           exposureOutcomes = .data$exposedCases,
           exposureDays = NA,
           counterfactualSubjects = (.data$cases + .data$controls) - (.data$exposedCases + .data$exposedControls),
           counterfactualOutcomes = .data$cases - .data$exposedCases,
           counterfactualDays = NA,
           llr = if_else(!is.na(.data$logRr) & .data$logRr < 0, 0, .data$llr)) %>%
    select(all_of(columns))
  
  ParallelLogger::logInfo("  - Adding SCCS / SCRI estimates")
  sccsEstimates <- loadEstimates(file.path(outputFolder, "sccsSummary_withCvs.csv")) %>%
    mutate(databaseId = !!databaseId,
           method = "SCCS",
           periodId = .data$seqId,
           exposureSubjects = .data$exposedSubjects,
           exposureOutcomes = .data$exposedOutcomes,
           exposureDays = .data$exposedDays,
           counterfactualSubjects = .data$outcomeSubjects,
           counterfactualOutcomes = .data$outcomeEvents - .data$exposedOutcomes,
           counterfactualDays = .data$daysObserved - .data$exposedDays,
           llr = if_else(!is.na(.data$logRr) & .data$logRr < 0, 0, .data$llr)) %>%
    select(all_of(columns))
  
  estimates <- bind_rows(historicComparatorEstimates,
                         cohortMethodEstimates,
                         caseControlEstimates,
                         sccsEstimates)
  
  ParallelLogger::logInfo("  - Performing empirical calibration on estimates using leave-one-out")
  exposures <- loadExposureCohorts(outputFolder) %>%
    select(.data$exposureId, .data$baseExposureId)
  
  allControls <- loadAllControls(outputFolder) %>%
    select(baseExposureId = .data$exposureId, .data$outcomeId, .data$oldOutcomeId, .data$targetEffectSize, .data$trueEffectSize) %>%
    inner_join(exposures, by = "baseExposureId") %>%
    select(-.data$baseExposureId)
  
  estimates <- estimates %>%
    inner_join(allControls, by = c("exposureId", "outcomeId"))
  
  cluster <- ParallelLogger::makeCluster(min(8, maxCores))
  ParallelLogger::clusterRequire(cluster, "dplyr")
  subsets <- split(estimates,
                   paste(estimates$exposureId, estimates$method, estimates$analysisId, estimates$periodId))
  rm(estimates)  # Free up memory
  results <- ParallelLogger::clusterApply(cluster,
                                          subsets,
                                          Eumaeus:::calibrate)
  ParallelLogger::stopCluster(cluster)
  rm(subsets)  # Free up memory
  columns <- c(columns, c("calibratedRr", "calibratedCi95Lb", "calibratedCi95Ub", "calibratedLogRr", "calibratedSeLogRr", "calibratedP"))
  results <- bind_rows(results) %>%
    select(all_of(columns))
  
  results <- enforceMinCellValue(results, "exposureSubjects", minCellCount)
  results <- enforceMinCellValue(results, "counterfactualSubjects", minCellCount)
  results <- enforceMinCellValue(results, "exposureOutcomes", minCellCount)
  results <- enforceMinCellValue(results, "counterfactualOutcomes", minCellCount)
  colnames(results) <- SqlRender::camelCaseToSnakeCase(colnames(results))
  fileName <- file.path(exportFolder, "estimate.csv")
  readr::write_csv(results, fileName)
  rm(results)  # Free up memory
}

calibratePLoo <- function(subset, leaveOutId) {
  subsetMinusOne <- subset[subset$oldOutcomeId != leaveOutId, ]
  one <- subset[subset$oldOutcomeId == leaveOutId, ]
  null <- EmpiricalCalibration::fitMcmcNull(logRr = subsetMinusOne$logRr[subsetMinusOne$targetEffectSize == 1], 
                                            seLogRr = subsetMinusOne$seLogRr[subsetMinusOne$targetEffectSize == 1])
  caliP <- EmpiricalCalibration::calibrateP(null = null,
                                            logRr = one$logRr,
                                            seLogRr = one$seLogRr)
  one$calibratedP <- caliP[, "p"]
  return(one)
}

calibrateCiLoo <- function(subset, leaveOutId) {
  subsetMinusOne <- subset[subset$oldOutcomeId != leaveOutId, ]
  one <- subset[subset$oldOutcomeId == leaveOutId, ]
  model <- EmpiricalCalibration::fitSystematicErrorModel(logRr = subsetMinusOne$logRr,
                                                         seLogRr = subsetMinusOne$seLogRr,
                                                         trueLogRr = log(subsetMinusOne$trueEffectSize),
                                                         estimateCovarianceMatrix = FALSE)
  calibratedCi <- EmpiricalCalibration::calibrateConfidenceInterval(logRr = one$logRr,
                                                                    seLogRr = one$seLogRr,
                                                                    model = model)
  one$calibratedRr <- exp(calibratedCi$logRr)
  one$calibratedCi95Lb <- exp(calibratedCi$logLb95Rr)
  one$calibratedCi95Ub <- exp(calibratedCi$logUb95Rr)
  one$calibratedLogRr <- calibratedCi$logRr
  one$calibratedSeLogRr <- calibratedCi$seLogRr
  return(one)
}

calibrate <- function(subset) {
  # subset <- subsets[[100]]
  # Performing calibration using leave-one-out (LOO) because this is a methods experiment.
  
  ncs <- subset %>%
    filter(.data$targetEffectSize == 1 & !is.na(.data$seLogRr))
  if (nrow(ncs) > 5) {
    subset <- purrr::map_dfr(unique(subset$oldOutcomeId), calibratePLoo, subset = subset)
  } else {
    subset$calibratedP <- rep(NA, nrow(subset))
  }
  
  pcs <- subset %>%
    filter(.data$targetEffectSize > 1 & !is.na(.data$seLogRr))
  
  if (nrow(pcs) > 5) {
    subset <- purrr::map_dfr(unique(subset$oldOutcomeId), calibrateCiLoo, subset = subset)
  } else {
    subset$calibratedRr <- rep(NA, nrow(subset))
    subset$calibratedCi95Lb <- rep(NA, nrow(subset))
    subset$calibratedCi95Ub <- rep(NA, nrow(subset))
    subset$calibratedLogRr <- rep(NA, nrow(subset))
    subset$calibratedSeLogRr <- rep(NA, nrow(subset))
  }
  return(subset)
}

exportDiagnostics <- function(outputFolder,
                              exportFolder,
                              databaseId,
                              minCellCount,
                              maxCores) {
  ParallelLogger::logInfo("Exporting diagnostics")
  ParallelLogger::logInfo("- covariate_balance table")
  fileName <- file.path(exportFolder, "covariate_balance.csv")
  if (file.exists(fileName)) {
    unlink(fileName)
  }
  first <- TRUE
  balanceFolder <- file.path(outputFolder, "balance")
  files <- list.files(balanceFolder, pattern = "bal_.*.rds", full.names = TRUE)
  pb <- txtProgressBar(style = 3)
  if (length(files) > 0) {
    for (i in 1:length(files)) {
      ids <- gsub("^.*bal_t", "", files[i])
      targetId <- as.numeric(gsub("_c.*", "", ids))
      ids <- gsub("^.*_c", "", ids)
      comparatorId <- as.numeric(gsub("_+[aso].*$", "", ids))
      if (grepl("_s", ids)) {
        subgroupId <- as.numeric(gsub("^.*_s", "", gsub("_a[0-9]*.rds", "", ids)))
      } else {
        subgroupId <- NA
      }
      if (grepl("_o", ids)) {
        outcomeId <- as.numeric(gsub("^.*_o", "", gsub("_a[0-9]*.rds", "", ids)))
      } else {
        outcomeId <- NA
      }
      ids <- gsub("^.*_a", "", ids)
      analysisId <- as.numeric(gsub(".rds", "", ids))
      balance <- readRDS(files[i])
      inferredTargetBeforeSize <- mean(balance$beforeMatchingSumTarget/balance$beforeMatchingMeanTarget,
                                       na.rm = TRUE)
      inferredComparatorBeforeSize <- mean(balance$beforeMatchingSumComparator/balance$beforeMatchingMeanComparator,
                                           na.rm = TRUE)
      inferredTargetAfterSize <- mean(balance$afterMatchingSumTarget/balance$afterMatchingMeanTarget,
                                      na.rm = TRUE)
      inferredComparatorAfterSize <- mean(balance$afterMatchingSumComparator/balance$afterMatchingMeanComparator,
                                          na.rm = TRUE)
      
      balance$databaseId <- databaseId
      balance$targetId <- targetId
      balance$comparatorId <- comparatorId
      balance$outcomeId <- outcomeId
      balance$analysisId <- analysisId
      balance <- balance[, c("databaseId",
                             "targetId",
                             "comparatorId",
                             "outcomeId",
                             "analysisId",
                             "covariateId",
                             "beforeMatchingMeanTarget",
                             "beforeMatchingMeanComparator",
                             "beforeMatchingStdDiff",
                             "afterMatchingMeanTarget",
                             "afterMatchingMeanComparator",
                             "afterMatchingStdDiff")]
      colnames(balance) <- c("databaseId",
                             "targetId",
                             "comparatorId",
                             "outcomeId",
                             "analysisId",
                             "covariateId",
                             "targetMeanBefore",
                             "comparatorMeanBefore",
                             "stdDiffBefore",
                             "targetMeanAfter",
                             "comparatorMeanAfter",
                             "stdDiffAfter")
      balance$targetMeanBefore[is.na(balance$targetMeanBefore)] <- 0
      balance$comparatorMeanBefore[is.na(balance$comparatorMeanBefore)] <- 0
      balance$stdDiffBefore <- round(balance$stdDiffBefore, 3)
      balance$targetMeanAfter[is.na(balance$targetMeanAfter)] <- 0
      balance$comparatorMeanAfter[is.na(balance$comparatorMeanAfter)] <- 0
      balance$stdDiffAfter <- round(balance$stdDiffAfter, 3)
      
      balance <- balance[!(round(balance$targetMeanBefore, 3) == 0 &
                             round(balance$comparatorMeanBefore, 3) == 0 &
                             round(balance$targetMeanAfter, 3) == 0 &
                             round(balance$comparatorMeanAfter, 3) == 0 &
                             round(balance$stdDiffBefore, 3) == 0 &
                             round(balance$stdDiffAfter, 3) == 0), ]
      
      balance <- enforceMinCellValue(balance,
                                     "targetMeanBefore",
                                     minCellCount/inferredTargetBeforeSize,
                                     TRUE)
      balance <- enforceMinCellValue(balance,
                                     "comparatorMeanBefore",
                                     minCellCount/inferredComparatorBeforeSize,
                                     TRUE)
      balance <- enforceMinCellValue(balance,
                                     "targetMeanAfter",
                                     minCellCount/inferredTargetAfterSize,
                                     TRUE)
      balance <- enforceMinCellValue(balance,
                                     "comparatorMeanAfter",
                                     minCellCount/inferredComparatorAfterSize,
                                     TRUE)
      balance$targetMeanBefore <- round(balance$targetMeanBefore, 3)
      balance$comparatorMeanBefore <- round(balance$comparatorMeanBefore, 3)
      balance$targetMeanAfter <- round(balance$targetMeanAfter, 3)
      balance$comparatorMeanAfter <- round(balance$comparatorMeanAfter, 3)
      
      balance <- balance[!is.na(balance$targetId), ]
      colnames(balance) <- SqlRender::camelCaseToSnakeCase(colnames(balance))
      write.table(x = balance,
                  file = fileName,
                  row.names = FALSE,
                  col.names = first,
                  sep = ",",
                  dec = ".",
                  qmethod = "double",
                  append = !first)
      first <- FALSE
      setTxtProgressBar(pb, i/length(files))
    }
  }
  close(pb)
  
  ParallelLogger::logInfo("- preference_score_dist table")
  reference <- readRDS(file.path(outputFolder, "cmOutput", "outcomeModelReference.rds"))
  preparePlot <- function(row, reference) {
    idx <- reference$analysisId == row$analysisId &
      reference$targetId == row$targetId &
      reference$comparatorId == row$comparatorId
    psFileName <- file.path(outputFolder,
                            "cmOutput",
                            reference$sharedPsFile[idx][1])
    if (file.exists(psFileName)) {
      ps <- readRDS(psFileName)
      if (length(unique(ps$treatment)) == 2 &&
          min(ps$propensityScore) < max(ps$propensityScore)) {
        ps <- CohortMethod:::computePreferenceScore(ps)
        
        pop1 <- ps$preferenceScore[ps$treatment == 1]
        pop0 <- ps$preferenceScore[ps$treatment == 0]
        
        bw1 <- ifelse(length(pop1) > 1, "nrd0", 0.1)
        bw0 <- ifelse(length(pop0) > 1, "nrd0", 0.1)
        
        d1 <- density(pop1, bw = bw1, from = 0, to = 1, n = 100)
        d0 <- density(pop0, bw = bw0, from = 0, to = 1, n = 100)
        
        result <- tibble::tibble(databaseId = databaseId,
                                 targetId = row$targetId,
                                 comparatorId = row$comparatorId,
                                 analysisId = row$analysisId,
                                 preferenceScore = d1$x,
                                 targetDensity = d1$y,
                                 comparatorDensity = d0$y)
        return(result)
      }
    }
    return(NULL)
  }
  subset <- unique(reference[reference$sharedPsFile != "",
                             c("targetId", "comparatorId", "analysisId")])
  data <- plyr::llply(split(subset, 1:nrow(subset)),
                      preparePlot,
                      reference = reference,
                      .progress = "text")
  data <- do.call("rbind", data)
  fileName <- file.path(exportFolder, "preference_score_dist.csv")
  if (!is.null(data)) {
    colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
  }
  readr::write_csv(data, fileName)
  
  
  ParallelLogger::logInfo("- propensity_model table")
  getPsModel <- function(row, reference) {
    idx <- reference$analysisId == row$analysisId &
      reference$targetId == row$targetId &
      reference$comparatorId == row$comparatorId
    psFileName <- file.path(outputFolder,
                            "cmOutput",
                            reference$sharedPsFile[idx][1])
    if (file.exists(psFileName)) {
      ps <- readRDS(psFileName)
      metaData <- attr(ps, "metaData")
      if (is.null(metaData$psError)) {
        cmDataFile <- file.path(outputFolder,
                                "cmOutput",
                                reference$cohortMethodDataFile[idx][1])
        cmData <- CohortMethod::loadCohortMethodData(cmDataFile)
        model <- CohortMethod::getPsModel(ps, cmData)
        model$covariateId[is.na(model$covariateId)] <- 0
        Andromeda::close(cmData)
        model$databaseId <- databaseId
        model$targetId <- row$targetId
        model$comparatorId <- row$comparatorId
        model$analysisId <- row$analysisId
        model <- model[, c("databaseId", "targetId", "comparatorId", "analysisId", "covariateId", "coefficient")]
        return(model)
      }
    }
    return(NULL)
  }
  subset <- unique(reference[reference$sharedPsFile != "",
                             c("targetId", "comparatorId", "analysisId")])
  data <- plyr::llply(split(subset, 1:nrow(subset)),
                      getPsModel,
                      reference = reference,
                      .progress = "text")
  data <- do.call("rbind", data)
  fileName <- file.path(exportFolder, "propensity_model.csv")
  if (!is.null(data)) {
    colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
  }
  readr::write_csv(data, fileName)
  
  
  ParallelLogger::logInfo("- kaplan_meier_dist table")
  ParallelLogger::logInfo("  Computing KM curves")
  reference <- readRDS(file.path(outputFolder, "cmOutput", "outcomeModelReference.rds"))
  outcomesOfInterest <- getOutcomesOfInterest()
  reference <- reference[reference$outcomeId %in% outcomesOfInterest, ]
  reference <- reference[, c("strataFile",
                             "studyPopFile",
                             "targetId",
                             "comparatorId",
                             "outcomeId",
                             "analysisId")]
  tempFolder <- file.path(exportFolder, "temp")
  if (!file.exists(tempFolder)) {
    dir.create(tempFolder)
  }
  cluster <- ParallelLogger::makeCluster(min(4, maxCores))
  ParallelLogger::clusterRequire(cluster, "ScyllaEstimation")
  tasks <- split(reference, seq(nrow(reference)))
  ParallelLogger::clusterApply(cluster,
                               tasks,
                               prepareKm,
                               outputFolder = outputFolder,
                               tempFolder = tempFolder,
                               databaseId = databaseId,
                               minCellCount = minCellCount)
  ParallelLogger::stopCluster(cluster)
  ParallelLogger::logInfo("  Writing to single csv file")
  saveKmToCsv <- function(file, first, outputFile) {
    data <- readRDS(file)
    if (!is.null(data)) {
      colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
    }
    write.table(x = data,
                file = outputFile,
                row.names = FALSE,
                col.names = first,
                sep = ",",
                dec = ".",
                qmethod = "double",
                append = !first)
  }
  outputFile <- file.path(exportFolder, "kaplan_meier_dist.csv")
  files <- list.files(tempFolder, "km_.*.rds", full.names = TRUE)
  if (length(files) > 0) {
    saveKmToCsv(files[1], first = TRUE, outputFile = outputFile)
    if (length(files) > 1) {
      plyr::l_ply(files[2:length(files)], saveKmToCsv, first = FALSE, outputFile = outputFile, .progress = "text")
    }
  }
  unlink(tempFolder, recursive = TRUE)
}

prepareKm <- function(task,
                      outputFolder,
                      tempFolder,
                      databaseId,
                      minCellCount) {
  ParallelLogger::logTrace("Preparing KM plot for target ",
                           task$targetId,
                           ", comparator ",
                           task$comparatorId,
                           ", outcome ",
                           task$outcomeId,
                           ", analysis ",
                           task$analysisId)
  outputFileName <- file.path(tempFolder, sprintf("km_t%s_c%s_o%s_a%s.rds",
                                                  task$targetId,
                                                  task$comparatorId,
                                                  task$outcomeId,
                                                  task$analysisId))
  if (file.exists(outputFileName)) {
    return(NULL)
  }
  popFile <- task$strataFile
  if (popFile == "") {
    popFile <- task$studyPopFile
  }
  population <- readRDS(file.path(outputFolder,
                                  "cmOutput",
                                  popFile))
  if (nrow(population) == 0  || length(unique(population$treatment)) != 2) {
    # Can happen when matching and treatment is predictable
    return(NULL)
  }
  data <- ScyllaEstimation:::prepareKaplanMeier(population)
  if (is.null(data)) {
    # No shared strata
    return(NULL)
  }
  data$targetId <- task$targetId
  data$comparatorId <- task$comparatorId
  data$outcomeId <- task$outcomeId
  data$analysisId <- task$analysisId
  data$databaseId <- databaseId
  data <- ScyllaEstimation:::enforceMinCellValue(data, "targetAtRisk", minCellCount)
  data <- ScyllaEstimation:::enforceMinCellValue(data, "comparatorAtRisk", minCellCount)
  saveRDS(data, outputFileName)
}

prepareKaplanMeier <- function(population) {
  dataCutoff <- 0.9
  population$y <- 0
  population$y[population$outcomeCount != 0] <- 1
  if (is.null(population$stratumId) || length(unique(population$stratumId)) == nrow(population)/2) {
    sv <- survival::survfit(survival::Surv(survivalTime, y) ~ treatment, population, conf.int = TRUE)
    idx <- summary(sv, censored = T)$strata == "treatment=1"
    survTarget <- tibble::tibble(time = sv$time[idx],
                                 targetSurvival = sv$surv[idx],
                                 targetSurvivalLb = sv$lower[idx],
                                 targetSurvivalUb = sv$upper[idx])
    idx <- summary(sv, censored = T)$strata == "treatment=0"
    survComparator <- tibble::tibble(time = sv$time[idx],
                                     comparatorSurvival = sv$surv[idx],
                                     comparatorSurvivalLb = sv$lower[idx],
                                     comparatorSurvivalUb = sv$upper[idx])
    data <- merge(survTarget, survComparator, all = TRUE)
  } else {
    population$stratumSizeT <- 1
    strataSizesT <- aggregate(stratumSizeT ~ stratumId, population[population$treatment == 1, ], sum)
    if (max(strataSizesT$stratumSizeT) == 1) {
      # variable ratio matching: use propensity score to compute IPTW
      if (is.null(population$propensityScore)) {
        stop("Variable ratio matching detected, but no propensity score found")
      }
      weights <- aggregate(propensityScore ~ stratumId, population, mean)
      if (max(weights$propensityScore) > 0.99999) {
        return(NULL)
      }
      weights$weight <- weights$propensityScore / (1 - weights$propensityScore)
    } else {
      # stratification: infer probability of treatment from subject counts
      strataSizesC <- aggregate(stratumSizeT ~ stratumId, population[population$treatment == 0, ], sum)
      colnames(strataSizesC)[2] <- "stratumSizeC"
      weights <- merge(strataSizesT, strataSizesC)
      if (nrow(weights) == 0) {
        warning("No shared strata between target and comparator")
        return(NULL)
      }
      weights$weight <- weights$stratumSizeT/weights$stratumSizeC
    }
    population <- merge(population, weights[, c("stratumId", "weight")])
    population$weight[population$treatment == 1] <- 1
    idx <- population$treatment == 1
    survTarget <- CohortMethod:::adjustedKm(weight = population$weight[idx],
                                            time = population$survivalTime[idx],
                                            y = population$y[idx])
    survTarget$targetSurvivalUb <- survTarget$s^exp(qnorm(0.975)/log(survTarget$s) * sqrt(survTarget$var)/survTarget$s)
    survTarget$targetSurvivalLb <- survTarget$s^exp(qnorm(0.025)/log(survTarget$s) * sqrt(survTarget$var)/survTarget$s)
    survTarget$targetSurvivalLb[survTarget$s > 0.9999] <- survTarget$s[survTarget$s > 0.9999]
    survTarget$targetSurvival <- survTarget$s
    survTarget$s <- NULL
    survTarget$var <- NULL
    idx <- population$treatment == 0
    survComparator <- CohortMethod:::adjustedKm(weight = population$weight[idx],
                                                time = population$survivalTime[idx],
                                                y = population$y[idx])
    survComparator$comparatorSurvivalUb <- survComparator$s^exp(qnorm(0.975)/log(survComparator$s) *
                                                                  sqrt(survComparator$var)/survComparator$s)
    survComparator$comparatorSurvivalLb <- survComparator$s^exp(qnorm(0.025)/log(survComparator$s) *
                                                                  sqrt(survComparator$var)/survComparator$s)
    survComparator$comparatorSurvivalLb[survComparator$s > 0.9999] <- survComparator$s[survComparator$s >
                                                                                         0.9999]
    survComparator$comparatorSurvival <- survComparator$s
    survComparator$s <- NULL
    survComparator$var <- NULL
    data <- merge(survTarget, survComparator, all = TRUE)
  }
  data <- data[, c("time", "targetSurvival", "targetSurvivalLb", "targetSurvivalUb", "comparatorSurvival", "comparatorSurvivalLb", "comparatorSurvivalUb")]
  cutoff <- quantile(population$survivalTime, dataCutoff)
  data <- data[data$time <= cutoff, ]
  if (cutoff <= 300) {
    xBreaks <- seq(0, cutoff, by = 50)
  } else if (cutoff <= 600) {
    xBreaks <- seq(0, cutoff, by = 100)
  } else {
    xBreaks <- seq(0, cutoff, by = 250)
  }
  
  targetAtRisk <- c()
  comparatorAtRisk <- c()
  for (xBreak in xBreaks) {
    targetAtRisk <- c(targetAtRisk,
                      sum(population$treatment == 1 & population$survivalTime >= xBreak))
    comparatorAtRisk <- c(comparatorAtRisk,
                          sum(population$treatment == 0 & population$survivalTime >=
                                xBreak))
  }
  data <- merge(data, tibble::tibble(time = xBreaks,
                                     targetAtRisk = targetAtRisk,
                                     comparatorAtRisk = comparatorAtRisk), all = TRUE)
  if (is.na(data$targetSurvival[1])) {
    data$targetSurvival[1] <- 1
    data$targetSurvivalUb[1] <- 1
    data$targetSurvivalLb[1] <- 1
  }
  if (is.na(data$comparatorSurvival[1])) {
    data$comparatorSurvival[1] <- 1
    data$comparatorSurvivalUb[1] <- 1
    data$comparatorSurvivalLb[1] <- 1
  }
  idx <- which(is.na(data$targetSurvival))
  while (length(idx) > 0) {
    data$targetSurvival[idx] <- data$targetSurvival[idx - 1]
    data$targetSurvivalLb[idx] <- data$targetSurvivalLb[idx - 1]
    data$targetSurvivalUb[idx] <- data$targetSurvivalUb[idx - 1]
    idx <- which(is.na(data$targetSurvival))
  }
  idx <- which(is.na(data$comparatorSurvival))
  while (length(idx) > 0) {
    data$comparatorSurvival[idx] <- data$comparatorSurvival[idx - 1]
    data$comparatorSurvivalLb[idx] <- data$comparatorSurvivalLb[idx - 1]
    data$comparatorSurvivalUb[idx] <- data$comparatorSurvivalUb[idx - 1]
    idx <- which(is.na(data$comparatorSurvival))
  }
  data$targetSurvival <- round(data$targetSurvival, 4)
  data$targetSurvivalLb <- round(data$targetSurvivalLb, 4)
  data$targetSurvivalUb <- round(data$targetSurvivalUb, 4)
  data$comparatorSurvival <- round(data$comparatorSurvival, 4)
  data$comparatorSurvivalLb <- round(data$comparatorSurvivalLb, 4)
  data$comparatorSurvivalUb <- round(data$comparatorSurvivalUb, 4)
  
  # Remove duplicate (except time) entries:
  data <- data[order(data$time), ]
  data <- data[!duplicated(data[, -1]), ]
  return(data)
}
