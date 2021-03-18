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
    
    exposureCohorts <- loadExposureCohorts(outputFolder) %>%
      filter(.data$sampled == FALSE & .data$comparator == FALSE)
    
    baseExposureIds <- exposureCohorts %>%
      distinct(.data$baseExposureId) %>%
      pull()
    
    allEstimates <- list()
    # baseExposureId <- baseExposureIds[1]
    for (baseExposureId in baseExposureIds) {
      exposures <- exposureCohorts %>%
        filter(.data$baseExposureId == !!baseExposureId) 
      
      controls <- allControls %>%
        filter(.data$exposureId == baseExposureId)
      
      exposureFolder <- file.path(hcFolder, sprintf("e_%s", baseExposureId))
      if (!file.exists(exposureFolder))
        dir.create(exposureFolder)
      
      historicRatesFile <- file.path(exposureFolder, "historicRates.rds")
      if (!file.exists(historicRatesFile)) {
        ParallelLogger::logInfo(sprintf("Computing historical rates for exposure %s", baseExposureId))
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
      
      timePeriods <- splitTimePeriod(startDate = controls$startDate[1], endDate = controls$endDate[1])
      # i <- 1
      for (i in 1:nrow(timePeriods)) {
        periodEstimatesFile <- file.path(exposureFolder, sprintf("estimates_t%d.csv", timePeriods$seqId[i]))
        if (!file.exists(periodEstimatesFile)) {
          periodEstimates <- list()
          # exposureId <- exposures$exposureId[1]
          for (exposureId in exposures$exposureId) {
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
            periodEstimates[[length(periodEstimates) + 1]] <- estimates
          }
          periodEstimates <- bind_rows(periodEstimates)
          readr::write_csv(periodEstimates, periodEstimatesFile)
        } else {
          periodEstimates <- loadEstimates(periodEstimatesFile)
        }
        periodEstimates$seqId <- timePeriods$seqId[i]
        periodEstimates$period <- timePeriods$label[i]
        allEstimates[[length(allEstimates) + 1]] <- periodEstimates
      }
    }
    allEstimates <- bind_rows(allEstimates)  
    readr::write_csv(allEstimates, hcSummaryFile)
  }
  delta <- Sys.time() - start
  message(paste("Completed historical comparator analyses in", signif(delta, 3), attr(delta, "units")))
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
  
  ParallelLogger::logInfo("- Computing population incidence rates without anchoring")
  sql <- SqlRender::loadRenderTranslateSql("ComputePopulationIncidenceRate.sql",
                                           "Eumaeus",
                                           dbms = connectionDetails$dbms,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable,
                                           cohort_ids = unique(outcomeIds),
                                           start_date = format(startDate, "%Y%m%d"),
                                           end_date = format(endDate, "%Y%m%d"),
                                           washout_period = 365,
                                           first_occurrence_only = TRUE,
                                           rate_type = "population-based")
  DatabaseConnector::executeSql(connection, sql)
  ratesNoAnchor <- DatabaseConnector::renderTranslateQuerySql(connection, "SELECT * FROM #rates;", snakeCaseToCamelCase = TRUE)
  ratesNoAnchor$tar <- "all time"
  
  ParallelLogger::logInfo("- Computing population incidence rates anchoring on visits. Time-at-risk is 1-28 days")
  sql <- SqlRender::loadRenderTranslateSql("ComputePopulationIncidenceRate.sql",
                                           "Eumaeus",
                                           dbms = connectionDetails$dbms,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable,
                                           cohort_ids = unique(outcomeIds),
                                           start_date = format(startDate, "%Y%m%d"),
                                           end_date = format(endDate, "%Y%m%d"),
                                           washout_period = 365,
                                           first_occurrence_only = TRUE,
                                           rate_type = "visit-based",
                                           visit_concept_ids = 9202,
                                           tar_start = 1,
                                           tar_end = 28)
  DatabaseConnector::executeSql(connection, sql)
  ratesVisit1_28 <- DatabaseConnector::renderTranslateQuerySql(connection, "SELECT * FROM #rates;", snakeCaseToCamelCase = TRUE)
  ratesVisit1_28$tar <- "1-28"
  
  ParallelLogger::logInfo("- Computing population incidence rates anchoring on visits. Time-at-risk is 1-42 days")
  sql <- SqlRender::loadRenderTranslateSql("ComputePopulationIncidenceRate.sql",
                                           "Eumaeus",
                                           dbms = connectionDetails$dbms,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable,
                                           cohort_ids = unique(outcomeIds),
                                           start_date = format(startDate, "%Y%m%d"),
                                           end_date = format(endDate, "%Y%m%d"),
                                           washout_period = 365,
                                           first_occurrence_only = TRUE,
                                           rate_type = "visit-based",
                                           visit_concept_ids = 9202,
                                           tar_start = 1,
                                           tar_end = 42)
  DatabaseConnector::executeSql(connection, sql)
  ratesVisit1_42 <- DatabaseConnector::renderTranslateQuerySql(connection, "SELECT * FROM #rates;", snakeCaseToCamelCase = TRUE)
  ratesVisit1_42$tar <- "1-42"
  
  ParallelLogger::logInfo("- Computing population incidence rates anchoring on visits. Time-at-risk is 0-1 days")
  sql <- SqlRender::loadRenderTranslateSql("ComputePopulationIncidenceRate.sql",
                                           "Eumaeus",
                                           dbms = connectionDetails$dbms,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable,
                                           cohort_ids = unique(outcomeIds),
                                           start_date = format(startDate, "%Y%m%d"),
                                           end_date = format(endDate, "%Y%m%d"),
                                           washout_period = 365,
                                           first_occurrence_only = TRUE,
                                           rate_type = "visit-based",
                                           visit_concept_ids = 9202,
                                           tar_start =0,
                                           tar_end = 1)
  DatabaseConnector::executeSql(connection, sql)
  ratesVisit0_1 <- DatabaseConnector::renderTranslateQuerySql(connection, "SELECT * FROM #rates;", snakeCaseToCamelCase = TRUE)
  ratesVisit0_1$tar <- "0-1"

  sql <- "TRUNCATE TABLE #rates; DROP TABLE #rates;"
  DatabaseConnector::renderTranslateExecuteSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  populationRates <- bind_rows(ratesNoAnchor,
                               ratesVisit1_28,
                               ratesVisit1_42,
                               ratesVisit0_1)
  
  
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


llr <- function(observed, expected) {
  result <- rep(0, length(observed))
  idx <- !is.na(observed) & !is.na(expected) & observed >= expected
  result[idx] <- (expected[idx] - observed[idx]) + observed[idx] * log(observed[idx] / expected[idx])
  return(result)
}

computeIrr <- function(outcomeId, ratesExposed, ratesBackground, adjusted = FALSE) {
  # outcomeId <- 432513        
  # print(outcomeId)
  
  target <- ratesExposed %>%
    filter(.data$outcomeId == !!outcomeId) 
  
  comparator <- ratesBackground %>%
    filter(.data$outcomeId == !!outcomeId) 
  
  estimateRow <- bind_cols(summarize(target, targetOutcomes = as.numeric(sum(.data$cohortCount)),
                           targetYears = sum(.data$personYears)),
                           summarize(comparator, comparatorOutcomes = as.numeric(sum(.data$cohortCount)),
                                     comparatorYears = sum(.data$personYears)))
 
  if (estimateRow$targetOutcomes == 0 || estimateRow$comparatorOutcomes == 0) {
    estimateRow$irr <- NA
    estimateRow$lb95Ci <- NA
    estimateRow$ub95Ci <- NA
    estimateRow$logRr <- NA
    estimateRow$seLogRr <- NA
    estimateRow$llr <- NA
  } else {
    if (adjusted) {
      target <- target %>%
        mutate(stratumId = paste(.data$ageGroup, .data$gender),
               exposed = 1)
      comparator <- target %>%
        mutate(stratumId = paste(.data$ageGroup, .data$gender),
               exposed = 0)
      data <- bind_rows(target, comparator)
      cyclopsData <- Cyclops::createCyclopsData(cohortCount ~ exposed + strata(stratumId) + offset(log(personYears)), 
                                                data = data, 
                                                modelType = "cpr")
      expectedOutcomes <- comparator %>%
        mutate(comparatorRate = .data$cohortCount / .data$personYears) %>%
        select(.data$comparatorRate, .data$stratumId) %>%
        inner_join(target, by = "stratumId") %>%
        mutate(expectedOutcomes = .data$personYears * .data$comparatorRate) %>%
        summarize(expectedOutcomes = sum(.data$expectedOutcomes)) %>%
        pull()
      
    } else {
      data <- tibble(cohortCount = c(estimateRow$targetOutcomes, estimateRow$comparatorOutcomes),
                     personYears = c(estimateRow$targetYears, estimateRow$comparatorYears),
                     exposed = c(1, 0))
      cyclopsData <- Cyclops::createCyclopsData(cohortCount ~ exposed + offset(log(personYears)), 
                                                data = data, 
                                                modelType = "pr")
      
      expectedOutcomes <- estimateRow$targetYears * (estimateRow$comparatorOutcomes / estimateRow$comparatorYears)
    }
    fit <- Cyclops::fitCyclopsModel(cyclopsData)
    if (fit$return_flag == "SUCCESS") {
      beta <- coef(fit)["exposed"]
      ci <- confint(fit, "exposed") 
    } else {
      beta <- NA
      ci <- c(NA, NA)
    }
    estimateRow$irr <- exp(beta)
    estimateRow$lb95Ci <- exp(ci[2])
    estimateRow$ub95Ci <- exp(ci[3])
    estimateRow$logRr <- beta
    estimateRow$seLogRr <- (ci[3] - ci[2]) / (qnorm(0.975) * 2)
    estimateRow$llr <- llr(estimateRow$targetOutcomes, expectedOutcomes)
  }
  estimateRow <- estimateRow %>%
    mutate(outcomeId = outcomeId)
  return(estimateRow)
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
  
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  estimates <- data.frame()
  tars <- data.frame(start = c(1, 1, 0),
                     end = c(28, 42, 1),
                     startAnalysisId = c(1, 5, 9))
  for (i in 1:nrow(tars)) {
    sql <- SqlRender::loadRenderTranslateSql("ComputePopulationIncidenceRate.sql",
                                             "Eumaeus",
                                             dbms = connectionDetails$dbms,
                                             cdm_database_schema = cdmDatabaseSchema,
                                             cohort_database_schema = cohortDatabaseSchema,
                                             cohort_table = cohortTable,
                                             cohort_ids = unique(outcomeIds),
                                             start_date = format(startDate, "%Y%m%d"),
                                             end_date = format(endDate, "%Y%m%d"),
                                             washout_period = 365,
                                             first_occurrence_only = TRUE,
                                             rate_type = "exposure-based",
                                             tar_start = tars$start[i],
                                             tar_end = tars$end[i],
                                             exposure_id = exposureId)
    DatabaseConnector::executeSql(connection, sql)
    ratesExposed <- DatabaseConnector::renderTranslateQuerySql(connection, "SELECT * FROM #rates;", snakeCaseToCamelCase = TRUE)
    
    # Unadjusted, no anchoring
    ratesBackground <- historicRates %>%
      filter(.data$tar == "all time")
  
    estimatesUnadjustedNoAnchoring <- purrr::map_dfr(outcomeIds, computeIrr, ratesExposed = ratesExposed, ratesBackground = ratesBackground, adjusted = FALSE) %>%
      mutate(analysisId = tars$startAnalysisId[i])
  
    # Age-sex adjusted, no anchoring
    estimatesAdjustedNoAnchoring <- purrr::map_dfr(outcomeIds, computeIrr, ratesExposed = ratesExposed, ratesBackground = ratesBackground, adjusted = TRUE) %>%
      mutate(analysisId = tars$startAnalysisId[i] + 1)
    
    # Unadjusted, visit anchoring
    ratesBackground <- historicRates %>%
      filter(.data$tar == sprintf("%d-%d", tars$start[i], tars$end[i]))
    
    estimatesUnadjustedVisitAnchoring <- purrr::map_dfr(outcomeIds, computeIrr, ratesExposed = ratesExposed, ratesBackground = ratesBackground, adjusted = FALSE) %>%
      mutate(analysisId = tars$startAnalysisId[i] + 2)
    
    # Age-sex adjusted, no anchoring
    estimatesAdjustedVisitAnchoring <- purrr::map_dfr(outcomeIds, computeIrr, ratesExposed = ratesExposed, ratesBackground = ratesBackground, adjusted = TRUE) %>%
      mutate(analysisId = tars$startAnalysisId[i] + 3)
    
    estimates <- bind_rows(estimatesUnadjustedNoAnchoring,
                           estimatesAdjustedNoAnchoring,
                           estimatesUnadjustedVisitAnchoring,
                           estimatesAdjustedVisitAnchoring,
                           estimates)
    
  }
  sql <- "TRUNCATE TABLE #rates; DROP TABLE #rates;"
  DatabaseConnector::renderTranslateExecuteSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  estimates <- estimates %>%
    mutate(exposureId = !!exposureId)
  
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Completed historical comparator estimates took", signif(delta, 3), attr(delta, "units")))
  return(estimates)
}
