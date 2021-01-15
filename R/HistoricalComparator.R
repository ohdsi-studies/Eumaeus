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

runHistoricalComparator <- function(connectionDetails,
                                    cdmDatabaseSchema,
                                    cohortDatabaseSchema,
                                    cohortTable,
                                    outputFolder,
                                    maxCores) {
  start <- Sys.time()
  hcFolder <- file.path(outputFolder, "historicalComparator")
  if (!file.exists(hcFolder))
    dir.create(hcFolder)
  
  hcSummaryFile <- file.path(outputFolder, "hcSummary.csv")
  if (!file.exists(hcSummaryFile)) {
    allControls <- loadAllControls(outputFolder)
    # controls <- allControls
    for (controls in split(allControls, allControls$exposureId)) {
      exposureId <- controls$exposureId[1]
      exposureFolder <- file.path(hcFolder, sprintf("e_%s", exposureId))
      if (!file.exists(exposureFolder))
        dir.create(exposureFolder)
      
      historicRatesFile <- file.path(exposureFolder, "historicRates.rds")
      if (!file.exists(historicRatesFile)) {
        ParallelLogger::logInfo(sprintf("Computing historical rates for exposure %s", exposureId))
        computeHistoricRates(connectionDetails = connectionDetails,
                             cdmDatabaseSchema = cdmDatabaseSchema,
                             cohortDatabaseSchema = cohortDatabaseSchema,
                             cohortTable = cohortTable,
                             startDate = controls$historyStartDate[1],
                             endDate = controls$historyEndDate[1],
                             outcomeIds = controls$oldOutcomeId,
                             newOutcomeIds = controls$outcomeId,
                             ratesFile = historicRatesFile)
      }
      
    }
    timePeriods <- splitTimePeriod(startDate = controls$startDate[1], endDate = controls$endDate[1])
    allEstimates <- list()
    # i <- 1
    for (i in 1:nrow(timePeriods)) {
      periodEstimatesFile <- file.path(exposureFolder, sprintf("estimates_t%d.csv", timePeriods$seqId[i]))
      if (!file.exists(periodEstimatesFile)) {
        ParallelLogger::logInfo(sprintf("Computing historical comparator estimates for exposure %s and period: %s", exposureId, timePeriods$label[i]))
        periodFolder <- file.path(exposureFolder, sprintf("historicalComparator_t%d", timePeriods$seqId[i]))
        estimates <- computeHistoricalComparatorEstimates(connectionDetails = connectionDetails,
                                                          cdmDatabaseSchema = cdmDatabaseSchema,
                                                          cohortDatabaseSchema = cohortDatabaseSchema,
                                                          cohortTable = cohortTable,
                                                          startDate = timePeriods$startDate[i],
                                                          endDate = timePeriods$endDate[i],
                                                          exposureId = exposureId,
                                                          outcomeIds = controls$outcomeId,
                                                          ratesFile = historicRatesFile,
                                                          periodFolder = periodFolder)
        readr::write_csv(estimates, periodEstimatesFile)
      } else {
        estimates <- readr::read_csv(periodEstimatesFile, col_types = readr::cols())
      }
      estimates$seqId <- timePeriods$seqId[i]
      estimates$period <- timePeriods$label[i]
      allEstimates[[length(allEstimates) + 1]] <- estimates
    }
    allEstimates <- bind_rows(allEstimates)  
    readr::write_csv(allEstimates, hcSummaryFile)
  }
  delta <- Sys.time() - start
  writeLines(paste("Completed historical comparator analyses in", signif(delta, 3), attr(delta, "units")))
  
  analysisDesc <- tibble(analysisId = c(1, 
                                        2),
                         description = c("Unadjusted historical comparator",
                                         "Age + sex adjusted historical comparator"))
  readr::write_csv(analysisDesc, file.path(outputFolder, "hcAnalysisDesc.csv"))
  
  # estimates <- readr::read_csv(periodEstimatesFile) %>%
  #   mutate(exposureId = bit64::as.integer64(.data$exposureId),
  #          outcomeId = bit64::as.integer64(.data$outcomeId))
  # combi <- estimates %>% 
  #   inner_join(allControls, by = c("exposureId", "outcomeId")) %>%
  #   filter(analysisId == 2)
  # ncs <- combi %>%
  #   filter(.data$trueEffectSize == 1)
  # EmpiricalCalibration::plotCalibrationEffect(logRrNegatives = ncs$logRr,
  #                                             seLogRrNegatives = ncs$seLogRr)
  # EmpiricalCalibration::plotCiCalibrationEffect(logRr = combi$logRr,
  #                                               seLogRr = combi$seLogRr,
  #                                               trueLogRr = log(combi$targetEffectSize))
}

computeHistoricRates <- function(connectionDetails,
                                 cdmDatabaseSchema,
                                 cohortDatabaseSchema,
                                 cohortTable,
                                 startDate,
                                 endDate,
                                 outcomeIds,
                                 newOutcomeIds,
                                 ratesFile) {
  start <- Sys.time()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  computeIr <- function(outcomeId) {
    ParallelLogger::logInfo("- Computing population incidence rates for outcome ", outcomeId)
    sql <- SqlRender::loadRenderTranslateSql("ComputePopulationIncidenceRate.sql",
                                             "VaccineSurveillanceMethodEvaluation",
                                             dbms = connectionDetails$dbms,
                                             cdm_database_schema = cdmDatabaseSchema,
                                             cohort_database_schema = cohortDatabaseSchema,
                                             cohort_table = cohortTable,
                                             cohort_id = outcomeId,
                                             start_date = format(startDate, "%Y%m%d"),
                                             end_date = format(endDate, "%Y%m%d"),
                                             washout_period = 365,
                                             first_occurrence_only = TRUE)
    DatabaseConnector::executeSql(connection, sql)
    outcomeRates <- DatabaseConnector::renderTranslateQuerySql(connection, "SELECT * FROM #rates_summary;", snakeCaseToCamelCase = TRUE)
    sql <- "TRUNCATE TABLE #rates_summary; DROP TABLE #rates_summary;"
    DatabaseConnector::renderTranslateExecuteSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
    outcomeRates$outcomeId <- outcomeId
    return(outcomeRates)
  }
  populationRates <- purrr::map_dfr(unique(outcomeIds), computeIr)
  mapping <- tibble(outcomeId = outcomeIds,
                    newOutcomeId = newOutcomeIds)
  populationRates <- populationRates %>%
    as_tibble() %>%
    mutate(type = "population") %>%
    inner_join(mapping, by = "outcomeId") %>%
    mutate(outcomeId = .data$newOutcomeId) %>%
    select(-.data$newOutcomeId)
  saveRDS(populationRates, ratesFile)
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Completed historical rates took", signif(delta, 3), attr(delta, "units")))
}

computeHistoricalComparatorEstimates <- function(connectionDetails,
                                                 cdmDatabaseSchema,
                                                 cohortDatabaseSchema,
                                                 cohortTable,
                                                 startDate,
                                                 endDate,
                                                 exposureId,
                                                 outcomeIds,
                                                 ratesFile,
                                                 periodFolder) {
  start <- Sys.time()
  historicRates <- readRDS(ratesFile)
  
  if (!file.exists(periodFolder)) {
    dir.create(periodFolder)
  }
  
  numeratorFile <- file.path(periodFolder, "numerator.rds")
  denominatorFile <- file.path(periodFolder, "denominator.rds")
  if (file.exists(numeratorFile) && file.exists(denominatorFile)) {
    numerator <- readRDS(numeratorFile)
    denominator <- readRDS(denominatorFile)
  } else {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
    
    ParallelLogger::logInfo("Fetching incidence rates during exposure")
    sql <- SqlRender::loadRenderTranslateSql("ComputeIncidenceRatesExposed.sql",
                                             "VaccineSurveillanceMethodEvaluation",
                                             dbms = connectionDetails$dbms,
                                             cdm_database_schema = cdmDatabaseSchema,
                                             cohort_database_schema = cohortDatabaseSchema,
                                             cohort_table = cohortTable,
                                             exposure_id = exposureId,
                                             outcome_ids = outcomeIds,
                                             start_date = format(startDate, "%Y%m%d"),
                                             end_date = format(endDate, "%Y%m%d"),
                                             washout_period = 365,
                                             first_occurrence_only = TRUE,
                                             time_at_risk_end = 30)
    DatabaseConnector::executeSql(connection, sql)
    numerator <- DatabaseConnector::renderTranslateQuerySql(connection, "SELECT * FROM #numerator;", snakeCaseToCamelCase = TRUE)
    denominator <- DatabaseConnector::renderTranslateQuerySql(connection, "SELECT * FROM #denominator;", snakeCaseToCamelCase = TRUE)
    sql <- "TRUNCATE TABLE #numerator; DROP TABLE #numerator; TRUNCATE TABLE #denominator; DROP TABLE #denominator;"
    DatabaseConnector::renderTranslateExecuteSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
    saveRDS(numerator, numeratorFile)
    saveRDS(denominator, denominatorFile)
  }
  
  llr <- function(observed, expected) {
    if (observed >= expected) {
      return((expected - observed) + observed * log(observed / expected))
    } else {
      return(0)
    }
  }
  
  # outcomeId <- 10003
  computeIrr <- function(outcomeId, adjusted = FALSE) {
    targetOutcomes <- numerator %>%
      filter(outcomeId == !!outcomeId) %>%
      summarize(count = as.numeric(sum(outcomeEvents))) %>%
      pull()
    targetYears <- denominator %>%
      summarize(count = sum(daysAtRisk) / 365.25) %>%
      pull()
    estimateRow <- historicRates %>% 
      filter(outcomeId == !!outcomeId) %>%
      summarize(comparatorOutcomes = as.numeric(sum(cohortCount)),
                comparatorYears = sum(personYears)) %>%
      mutate(targetOutcomes = targetOutcomes,
             targetYears = targetYears)
    
    estimateRow$expectedOutcomes <- estimateRow$targetYears * (estimateRow$comparatorOutcomes / estimateRow$comparatorYears)
    
    if (estimateRow$targetOutcomes == 0) {
      estimateRow$irr <- NA
      estimateRow$lb95Ci <- NA
      estimateRow$ub95Ci <- NA
      estimateRow$logRr <- NA
      estimateRow$seLogRr <- NA
    } else {
      if (adjusted) {
        dataTarget <- denominator %>%
          left_join(filter(numerator, outcomeId == !!outcomeId), by = c("ageGroup", "genderConceptId")) %>%
          mutate(outcomes = ifelse(is.na(.data$outcomeEvents), 0, as.numeric(.data$outcomeEvents)),
                 years = .data$daysAtRisk / 365.25,
                 stratumId = paste(.data$ageGroup, .data$gender),
                 exposed = 1) %>%
          select(.data$outcomes, .data$years, .data$stratumId, .data$exposed)
        dataComparator <- historicRates %>%
          filter(outcomeId == !!outcomeId) %>%
          mutate(outcomes = as.numeric(.data$cohortCount),
                 years = .data$personYears,
                 stratumId = paste(.data$ageGroup, .data$gender),
                 exposed = 0) %>%
          select(.data$outcomes, .data$years, .data$stratumId, .data$exposed)
        data <- bind_rows(dataTarget, dataComparator)
        cyclopsData <- Cyclops::createCyclopsData(outcomes ~ exposed + strata(stratumId) + offset(log(years)), 
                                                  data = data, 
                                                  modelType = "cpr")
        
        estimateRow$expectedOutcomes <- dataComparator %>%
          mutate(comparatorRate = .data$outcomes / .data$years) %>%
          select(.data$comparatorRate, .data$stratumId) %>%
          inner_join(dataTarget, by = "stratumId") %>%
          mutate(expectedOutcomes = years * comparatorRate) %>%
          summarize(expectedOutcomes = sum(expectedOutcomes)) %>%
          pull()
        
        
      } else {
        data <- tibble(outcomes = c(estimateRow$targetOutcomes, estimateRow$comparatorOutcomes),
                       years = c(estimateRow$targetYears, estimateRow$comparatorYears),
                       exposed = c(1, 0))
        cyclopsData <- Cyclops::createCyclopsData(outcomes ~ exposed + offset(log(years)), 
                                                  data = data, 
                                                  modelType = "pr")
      }
      fit <- Cyclops::fitCyclopsModel(cyclopsData)
      beta <- coef(fit)["exposed"]
      ci <- confint(fit, "exposed") 
      estimateRow$irr <- exp(beta)
      estimateRow$lb95Ci <- exp(ci[2])
      estimateRow$ub95Ci <- exp(ci[3])
      estimateRow$logRr <- beta
      estimateRow$seLogRr <- (ci[3] - ci[2]) / (qnorm(0.975) * 2)
    }
    estimateRow$llr <- llr(estimateRow$targetOutcomes, estimateRow$expectedOutcomes)
    estimateRow <- estimateRow %>%
      mutate(exposureId = exposureId,
             outcomeId = outcomeId,
             analysisId = ifelse(adjusted, 2, 1))
    return(estimateRow)
  }
  estimatesUnadjusted <- purrr::map_dfr(outcomeIds, computeIrr, adjusted = FALSE)
  estimatesAdjusted <- purrr::map_dfr(outcomeIds, computeIrr, adjusted = TRUE)
  estimates <- bind_rows(estimatesUnadjusted, estimatesAdjusted)
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Completed historical comparator estimates took", signif(delta, 3), attr(delta, "units")))
  return(estimates)
}

computeIrr <- function() {
  oe <- observedExpected[observedExpected$outcomeId == 10003, ]
  oe <- data.frame(outcomeEvents = c(1, 2),
                   exposed = c(1, 0),
                   daysAtRisk = c(1000, 1000))
  fit <- glm(outcomeEvents ~ exposed + offset(log(daysAtRisk)), data = oe, family = "poisson")
  cyclopsData <- Cyclops::createCyclopsData(outcomeEvents ~ exposed + survival::strata(outcomeId) + offset(log(daysAtRisk)), 
                                            data = observedExpected, 
                                            modelType = "cpr")
  
  cyclopsData <- Cyclops::createCyclopsData(outcomeEvents ~ exposed + offset(log(daysAtRisk)), 
                                            data = oe, 
                                            modelType = "pr")
  
  fit <- Cyclops::fitCyclopsModel(cyclopsData)
  coef(fit)
  confint(fit, "exposed") 
  
}

