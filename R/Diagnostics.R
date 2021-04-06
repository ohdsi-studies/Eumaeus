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

generateDiagnostics <- function(outputFolder,
                                connectionDetails,
                                cdmDatabaseSchema,
                                cohortDatabaseSchema,
                                cohortTable,
                                maxCores) {
  
  generateHcDiagnostics(connectionDetails = connectionDetails,
                        cdmDatabaseSchema = cdmDatabaseSchema,
                        cohortDatabaseSchema = cohortDatabaseSchema,
                        cohortTable = cohortTable,
                        outputFolder = outputFolder,
                        maxCores = maxCores)
  
}

generateHcDiagnostics <- function(outputFolder,
                                  connectionDetails,
                                  cdmDatabaseSchema,
                                  cohortDatabaseSchema,
                                  cohortTable,
                                  maxCores) {
  fileName <- file.path(outputFolder, "hcDiagnosticsRates.csv")
  if (!file.exists(fileName)) {
    ParallelLogger::logInfo("Computing diagnostics for historic comparator")
    
    exposuresOfInterest <- loadExposuresofInterest()
    allControls <- loadAllControls(outputFolder)
    startDate <- min(exposuresOfInterest$historyStartDate) - 366
    endDate <- max(exposuresOfInterest$endDate) 
    periods <- splitTimePeriod(startDate, endDate)
    periods$startDate[2:nrow(periods)] <- periods$endDate[1:(nrow(periods) - 1)] + 1
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
    
    computeRatesForPeriod <- function(i) {
      ParallelLogger::logInfo(sprintf("- Computing rates for period from %s to %s", format(periods$startDate[i]), format(periods$endDate[i])))
      sql <- SqlRender::loadRenderTranslateSql("ComputePopulationIncidenceRate.sql",
                                               "Eumaeus",
                                               dbms = connectionDetails$dbms,
                                               cdm_database_schema = cdmDatabaseSchema,
                                               cohort_database_schema = cohortDatabaseSchema,
                                               cohort_table = cohortTable,
                                               cohort_ids = allControls$outcomeId,
                                               start_date = format(periods$startDate[i], "%Y%m%d"),
                                               end_date = format(periods$endDate[i], "%Y%m%d"),
                                               washout_period = 365,
                                               first_occurrence_only = TRUE,
                                               rate_type = "population-based")
      DatabaseConnector::executeSql(connection, sql)
      rates <- DatabaseConnector::renderTranslateQuerySql(connection, "SELECT * FROM #rates;", snakeCaseToCamelCase = TRUE)
      rates <- rates %>%
        group_by(.data$outcomeId) %>%
        summarize(cohortCount = sum(.data$cohortCount),
                  personYears = sum(.data$personYears))
      rates$startDate <- periods$startDate[i]
      rates$endDate <- periods$endDate[i]
      return(rates)
    }
    allRates <- purrr::map_dfr(1:nrow(periods), computeRatesForPeriod)
    sql <- "TRUNCATE TABLE #rates; DROP TABLE #rates;"
    DatabaseConnector::renderTranslateExecuteSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
    readr::write_csv(allRates, fileName)
  }
}