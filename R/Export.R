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
  
  exportDiagnostics(outputFolder = outputFolder,
                    exportFolder = exportFolder,
                    databaseId = databaseId,
                    minCellCount = minCellCount,
                    maxCores = maxCores)
  
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
    select(.data$exposureId, 
           .data$exposureName, 
           totalShots = .data$shots, 
           .data$baseExposureId, 
           .data$baseExposureName, 
           .data$shot, 
           .data$startDate, 
           .data$endDate, 
           .data$historyStartDate , 
           .data$historyEndDate)
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
  
  database <- tibble(databaseId = databaseId,
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
  table <- enforceMinCellValue(table, "subjectCount", minValues = minCellCount)
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
  columns <- c("databaseId",  "method", "analysisId", "exposureId", "outcomeId", "periodId", "rr", "ci95Lb", "ci95Ub", "p", "exposureSubjects", "counterfactualSubjects", "exposureDays", "counterfactualDays", "exposureOutcomes", "counterfactualOutcomes", "logRr", "seLogRr", "llr", "criticalValue")
  
  ParallelLogger::logInfo("  - Adding historical comparator estimates")
  historicComparatorEstimates <- loadEstimates(file.path(outputFolder, "hcSummary_withCvs.csv")) %>%
    mutate(databaseId = !!databaseId,
           method = "HistoricalComparator",
           periodId = .data$seqId,
           exposureSubjects = .data$targetSubjects,
           exposureOutcomes = .data$targetOutcomes,
           exposureDays = round(.data$targetYears * 365.25),
           counterfactualSubjects = .data$comparatorSubjects,
           counterfactualOutcomes = .data$comparatorOutcomes,
           counterfactualDays = round(.data$comparatorYears * 365.25),
           rr = .data$irr,
           ci95Lb = .data$lb95Ci,
           ci95Ub = .data$ub95Ci,
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
           ci95Lb = .data$ci95lb,
           ci95Ub = .data$ci95ub,
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
           ci95Lb = .data$ci95lb,
           ci95Ub = .data$ci95ub,
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
           ci95Lb = .data$ci95lb,
           ci95Ub = .data$ci95ub,
           llr = if_else(!is.na(.data$logRr) & .data$logRr < 0, 0, .data$llr)) %>%
    select(all_of(columns))
  
  estimates <- bind_rows(historicComparatorEstimates,
                         cohortMethodEstimates,
                         caseControlEstimates,
                         sccsEstimates)
  
  ParallelLogger::logInfo("  - Performing empirical calibration on estimates using leave-one-out")
  # Try to reuse previous calibration to save time:
  fileName <- file.path(exportFolder, "estimate.csv")
  if (file.exists(fileName)) {
    estimatesWithCalibration <- readr::read_csv(fileName, col_types = readr::cols(), guess_max = 1e5)  
    colnames(estimatesWithCalibration) <- SqlRender::snakeCaseToCamelCase(colnames(estimatesWithCalibration))
    newGroups <- estimates %>%
      anti_join(select(estimatesWithCalibration, 
                       .data$exposureId, 
                       .data$outcomeId, 
                       .data$periodId,
                       .data$method,
                       .data$analysisId,
                       .data$logRr,
                       .data$seLogRr),
                by = c("method", "analysisId", "exposureId", "outcomeId", "periodId", "logRr", "seLogRr")) %>%
      distinct(.data$exposureId, 
               .data$periodId,
               .data$method,
               .data$analysisId)
    
    newEstimates <- estimates %>%
      inner_join(newGroups, by = c("method", "analysisId", "exposureId", "periodId"))
    
    oldEstimatesWithCalibration <- estimatesWithCalibration %>%
      inner_join(select(estimates, 
             .data$exposureId, 
             .data$outcomeId, 
             .data$periodId,
             .data$method,
             .data$analysisId,
             .data$logRr,
             .data$seLogRr),
             by = c("method", "analysisId", "exposureId", "outcomeId", "periodId", "logRr", "seLogRr")) %>%
      anti_join(newGroups, by = c("method", "analysisId", "exposureId", "periodId"))
    
    if (nrow(newEstimates) + nrow(oldEstimatesWithCalibration) != nrow(estimates)) {
      stop("Error reusing calibration")
    }
  } else {
    newEstimates <- estimates
    oldEstimatesWithCalibration <- tibble()
  }
  
  if (nrow(newEstimates) > 0) { 
    exposures <- loadExposureCohorts(outputFolder) %>%
      select(.data$exposureId, .data$baseExposureId)
    
    allControls <- loadAllControls(outputFolder) %>%
      select(baseExposureId = .data$exposureId, .data$outcomeId, .data$oldOutcomeId, .data$targetEffectSize, .data$trueEffectSize) %>%
      inner_join(exposures, by = "baseExposureId") %>%
      select(-.data$baseExposureId)
    
    newEstimates <- newEstimates %>%
      inner_join(allControls, by = c("exposureId", "outcomeId"))
    
    cluster <- ParallelLogger::makeCluster(min(8, maxCores))
    ParallelLogger::clusterRequire(cluster, "dplyr")
    subsets <- split(newEstimates,
                     paste(newEstimates$exposureId, newEstimates$method, newEstimates$analysisId, newEstimates$periodId))
    rm(newEstimates)  # Free up memory
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
    # results <- enforceMinCellValue(results, "exposureOutcomes", minCellCount)
    results <- enforceMinCellValue(results, "counterfactualOutcomes", minCellCount)
    
    estimatesWithCalibration <- bind_rows(oldEstimatesWithCalibration, results)
    colnames(estimatesWithCalibration) <- SqlRender::camelCaseToSnakeCase(colnames(estimatesWithCalibration))
    readr::write_csv(estimatesWithCalibration, fileName)
    rm(results)  # Free up memory
  }
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
  
  ParallelLogger::logInfo("- historical_rate table")
  
  folders <- list.files(file.path(outputFolder, "historicalComparator"), "^e_[0-9]+$", include.dirs = TRUE)
  getHistoricalRates <- function(folder) {
    rates <- readRDS(file.path(outputFolder, "historicalComparator", folder, "historicRates.rds"))
    rates <- rates %>%
      transmute(databaseId = databaseId,
                exposureId = as.numeric(gsub("^e_", "", folder)),
                .data$outcomeId,
                timeAtRisk = .data$tar,
                ageGroup = sprintf("%d-%d", .data$ageGroup * 10, .data$ageGroup * 10+ 9),
                .data$gender,
                outcomes = .data$cohortCount,
                days = round(.data$personYears * 365.25),
                subjects = .data$personCount)
  }
  results <- purrr::map_dfr(folders, getHistoricalRates)
  # aggregate across age and gender:
  resultsAggregated <- results %>%
    group_by(.data$outcomeId, .data$exposureId, .data$databaseId, .data$timeAtRisk) %>%
    summarize(outcomes = sum(.data$outcomes),
              days = sum(.data$days),
              .groups = "drop") %>%
    mutate(ageGroup = "",
           gender = "",
           subjects  = NA)
  results <- rbind(results, resultsAggregated)
  results <- enforceMinCellValue(results, "outcomes", minCellCount)
  results <- enforceMinCellValue(results, "subjects", minCellCount)
  colnames(results) <- SqlRender::camelCaseToSnakeCase(colnames(results))
  fileName <- file.path(exportFolder, "historical_rate.csv")
  readr::write_csv(results, fileName)
  
  ParallelLogger::logInfo("- monthly_rate table")
  fileName <- file.path(outputFolder, "hcDiagnosticsRates.csv")
  montlyRates <- readr::read_csv(fileName, col_types = readr::cols())
  montlyRates <- montlyRates %>%
    mutate(databaseId = !!databaseId,
           outcomes = .data$cohortCount,
           days = round(.data$personYears * 365.25)) %>%
    select(-.data$cohortCount, -.data$personYears)
  montlyRates <- enforceMinCellValue(montlyRates, "outcomes", minCellCount)
  colnames(montlyRates) <- SqlRender::camelCaseToSnakeCase(colnames(montlyRates))
  fileName <- file.path(exportFolder, "monthly_rate.csv")
  readr::write_csv(montlyRates, fileName)
}
  