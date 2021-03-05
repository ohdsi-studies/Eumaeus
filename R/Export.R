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


#' Export all results to tables
#'
#' @description
#' Outputs all results to a folder called 'export', and zips them.
#'
#' @param outputFolder          Name of local folder to place results; make sure to use forward slashes
#'                              (/). Do not use a folder on a network drive since this greatly impacts
#'                              performance.
#' @param databaseId            A short string for identifying the database (e.g. 'Synpuf').
#' @param databaseName          The full name of the database.
#' @param databaseDescription   A short description (several sentences) of the database.
#' @param minCellCount          The minimum cell count for fields contains person counts or fractions.
#' @param maxCores              How many parallel cores should be used? If more cores are made
#'                              available this can speed up the analyses.
#'
#' @export
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
  zipName <- file.path(exportFolder, sprintf("Results_%s.zip", databaseId))
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
    select(.data$exposureId, .data$exposureName, .data$shots, .data$baseExposureId, .data$baseExposureName, .data$shot)
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
  # SCCS
  estimates <- loadEstimates(file.path(outputFolder, "sccsSummary.csv")) %>%
    mutate(method = "SCCS")
  
  ParallelLogger::logInfo("  Performing empirical calibration on estimates")
  exposures <- loadExposureCohorts(outputFolder) %>%
    select(.data$exposureId, .data$baseExposureId)
  
  allControls <- loadAllControls(outputFolder) %>%
    select(baseExposureId = .data$exposureId, .data$outcomeId, .data$oldOutcomeId, .data$targetEffectSize, .data$trueEffectSize) %>%
    inner_join(exposures, by = "baseExposureId") %>%
    select(-.data$baseExposureId)
  cluster <- ParallelLogger::makeCluster(min(4, maxCores))
  ParallelLogger::clusterRequire(cluster, "dplyr")
  subsets <- split(estimates,
                   paste(estimates$exposureId, estimates$method, estimates$analysisId, estimates$periodId))
  rm(estimates)  # Free up memory
  results <- ParallelLogger::clusterApply(cluster,
                                          subsets,
                                          calibrate,
                                          allControls = allControls)
  ParallelLogger::stopCluster(cluster)
  rm(subsets)  # Free up memory
  results <- bind_rows(results)
  results$databaseId <- databaseId
  results <- enforceMinCellValue(results, "targetSubjects", minCellCount)
  results <- enforceMinCellValue(results, "comparatorSubjects", minCellCount)
  results <- enforceMinCellValue(results, "targetOutcomes", minCellCount)
  results <- enforceMinCellValue(results, "comparatorOutcomes", minCellCount)
  colnames(results) <- SqlRender::camelCaseToSnakeCase(colnames(results))
  fileName <- file.path(exportFolder, "cohort_method_result.csv")
  readr::write_csv(results, fileName)
  rm(results)  # Free up memory
  
  ParallelLogger::logInfo("- likelihood_profile table")
  reference <- readRDS(file.path(outputFolder, "cmOutput", "outcomeModelReference.rds"))
  fileName <- file.path(exportFolder, "likelihood_profile.csv")
  if (file.exists(fileName)) {
    unlink(fileName)
  }
  first <- TRUE
  pb <- txtProgressBar(style = 3)
  for (i in 1:nrow(reference)) {
    if (reference$outcomeModelFile[i] != "") {
      outcomeModel <- readRDS(file.path(outputFolder, "cmOutput", reference$outcomeModelFile[i]))
      profile <- outcomeModel$logLikelihoodProfile
      if (!is.null(profile)) {
        # Custom approximation:
        # fit <- EvidenceSynthesis:::fitLogLikelihoodFunction(beta = as.numeric(names(profile)),
        #                                                     ll = profile)
        # profile <- data.frame(targetId = reference$targetId[i],
        #                       comparatorId = reference$comparatorId[i],
        #                       outcomeId = reference$outcomeId[i],
        #                       analysisId = reference$analysisId[i],
        #                       mu = fit$mu,
        #                       sigma = fit$sigma,
        #                       gamma = fit$gamma)
        # colnames(profile) <- SqlRender::camelCaseToSnakeCase(colnames(profile))
        
        # Grid approximation using many columns (Postgres doesn't like this):
        # profile <- as.data.frame(t(round(profile, 4)))
        # colnames(profile) <- paste0("x_", gsub("\\.", "_", gsub("-", "min", sprintf("%0.6f", as.numeric(colnames(profile))))))
        
        
        # Grid approximation using one free-text column:
        profile <- data.frame(profile = paste(round(profile, 4), collapse = ";"))
        
        profile$target_id <- reference$targetId[i]
        profile$comparator_id <- reference$comparatorId[i]
        profile$outcome_id <- reference$outcomeId[i]
        profile$analysis_id <- reference$analysisId[i]
        profile$database_id <- databaseId
        
        # Grid approximation using many rows: (too inefficient)
        # profile <- data.frame(targetId = reference$targetId[i],
        #                       comparatorId = reference$comparatorId[i],
        #                       outcomeId = reference$outcomeId[i],
        #                       analysisId = reference$analysisId[i],
        #                       logHazardRatio = round(as.numeric(names(profile)), 4),
        #                       logLikelihood = round(profile - max(profile), 4))
        # colnames(profile) <- SqlRender::camelCaseToSnakeCase(colnames(profile))
        write.table(x = profile,
                    file = fileName,
                    row.names = FALSE,
                    col.names = first,
                    sep = ",",
                    dec = ".",
                    qmethod = "double",
                    append = !first)
        first <- FALSE
      }
    }
    setTxtProgressBar(pb, i/nrow(reference))
  }
  close(pb)
}

calibrate <- function(subset, allControls) {
  # subset <- estimates[estimates$exposureId == 205023 & estimates$method == "SCCS" & estimates$analysisId == 2 & estimates$periodId == 3, ]
  subset <- subset %>%
    inner_join(allControls, by = c("exposureId", "outcomeId"))
  
  ncs <- subset %>%
    filter(.data$targetEffectSize == 1 & !is.na(.data$seLogRr))
  if (nrow(ncs) > 5) {
    null <- EmpiricalCalibration::fitMcmcNull(ncs$logRr, ncs$seLogRr)
    calibratedP <- EmpiricalCalibration::calibrateP(null = null,
                                                    logRr = subset$logRr,
                                                    seLogRr = subset$seLogRr)
    subset$calibratedP <- calibratedP$p
  } else {
    subset$calibratedP <- rep(NA, nrow(subset))
  }
  
  pcs <- subset %>%
    inner_join(allControls, by = c("exposureId", "outcomeId")) %>%
    filter(.data$targetEffectSize > 1 & !is.na(.data$seLogRr))
  
  if (nrow(pcs) > 5) {
    model <- EmpiricalCalibration::fitSystematicErrorModel(logRr = c(ncs$logRr, pcs$logRr),
                                                           seLogRr = c(ncs$seLogRr,
                                                                       pcs$seLogRr),
                                                           trueLogRr = c(rep(0, nrow(ncs)),
                                                                         log(pcs$trueEffectSize)),
                                                           estimateCovarianceMatrix = FALSE)
    calibratedCi <- EmpiricalCalibration::calibrateConfidenceInterval(logRr = subset$logRr,
                                                                      seLogRr = subset$seLogRr,
                                                                      model = model)
    subset$calibratedRr <- exp(calibratedCi$logRr)
    subset$calibratedCi95Lb <- exp(calibratedCi$logLb95Rr)
    subset$calibratedCi95Ub <- exp(calibratedCi$logUb95Rr)
    subset$calibratedLogRr <- calibratedCi$logRr
    subset$calibratedSeLogRr <- calibratedCi$seLogRr
  } else {
    subset$calibratedP <- rep(NA, nrow(subset))
    subset$calibratedRr <- rep(NA, nrow(subset))
    subset$calibratedCi95Lb <- rep(NA, nrow(subset))
    subset$calibratedCi95Ub <- rep(NA, nrow(subset))
    subset$calibratedLogRr <- rep(NA, nrow(subset))
    subset$calibratedSeLogRr <- rep(NA, nrow(subset))
  }
  subset <- subset[, c("exposureId",
                       "outcomeId",
                       "analysisId",
                       "periodId",
                       "rr",
                       "ci95Lb",
                       "ci95Ub",
                       "p",
                       "logRr",
                       "seLogRr",
                       "calibratedP",
                       "calibratedRr",
                       "calibratedCi95Lb",
                       "calibratedCi95Ub",
                       "calibratedLogRr",
                       "calibratedSeLogRr",
                       "exposedSubjects",
                       "exposedDays",
                       "exposedOutcomes",
                       "expectedOutcomes",
                       "llr")]
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
