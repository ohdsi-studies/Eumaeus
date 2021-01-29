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

computeRatesNearEndOfObs <- function(cdmDatabaseSchema,
                                     cohortDatabaseSchema,
                                     cohortTable) {
  start <- Sys.time()
  
  
  negativeControls <- loadNegativeControls()
  
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  # outcomeId <- negativeControls$outcomeId[1]
  computeIr <- function(outcomeId) {
    ParallelLogger::logInfo("- Computing population incidence rates for outcome ", outcomeId)
    sql <- SqlRender::loadRenderTranslateSql("ComputeIncidenceRatesNearDbEnd.sql",
                                             "VaccineSurveillanceMethodEvaluation",
                                             dbms = connectionDetails$dbms,
                                             cdm_database_schema = cdmDatabaseSchema,
                                             cohort_database_schema = cohortDatabaseSchema,
                                             cohort_table = cohortTable,
                                             cohort_id = outcomeId,
                                             washout_period = 365,
                                             first_occurrence_only = TRUE)
    DatabaseConnector::executeSql(connection, sql)
    outcomeRates <- DatabaseConnector::renderTranslateQuerySql(connection, "SELECT * FROM #rates_summary;", snakeCaseToCamelCase = TRUE)
    sql <- "TRUNCATE TABLE #rates_summary; DROP TABLE #rates_summary;"
    DatabaseConnector::renderTranslateExecuteSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
    outcomeRates$outcomeId <- outcomeId
    return(outcomeRates)
  }
  populationRates <- purrr::map_dfr(unique(negativeControls$outcomeId), computeIr)
  
  
  irs <- populationRates %>%
    group_by(monthsToDbEnd, outcomeId) %>%
    summarize(cohortCount = sum(.data$cohortCount),
              personYears = sum(.data$personYears)) %>%
    mutate(rate = cohortCount / (personYears * 100000))
  
  ggplot2::ggplot(irs, ggplot2::aes(x = .data$monthsToDbEnd, y = .data$rate, group = .data$outcomeId)) +
                    ggplot2::geom_line()
 
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Completed historical rates took", signif(delta, 3), attr(delta, "units")))
}
