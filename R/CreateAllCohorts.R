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

createCohorts <- function(connectionDetails,
                          cdmDatabaseSchema,
                          cohortDatabaseSchema,
                          cohortTable,
                          outputFolder) {
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  ParallelLogger::logInfo("Creating base expsosure cohorts")
  .createCohorts(connection = connection,
                 cdmDatabaseSchema = cdmDatabaseSchema,
                 cohortDatabaseSchema = cohortDatabaseSchema,
                 cohortTable = cohortTable,
                 outputFolder = outputFolder)
  
  ParallelLogger::logInfo("Creating derived exposure cohorts")
  exposuresOfInterest <- loadExposuresofInterest()
  derivedExposures <- purrr::map_dfr(split(exposuresOfInterest, 1:nrow(exposuresOfInterest)), 
                                     deriveExposureCohorts,
                                     connection = connection,
                                     cohortDatabaseSchema = cohortDatabaseSchema,
                                     cohortTable = cohortTable)
  readr::write_csv(derivedExposures, file.path(outputFolder, "DerivedExposures.csv"))
  # derivedExposures <- readr::read_csv(file.path(outputFolder, "DerivedExposures.csv"))
  
  ParallelLogger::logInfo("Creating non-user comparator cohorts")
  allExposureCohorts <- purrr::map_dfr(split(derivedExposures, 1:nrow(derivedExposures)), 
                                       sampleComparatorCohorts,
                                       connection = connection,
                                       cdmDatabaseSchema = cdmDatabaseSchema,
                                       cohortDatabaseSchema = cohortDatabaseSchema,
                                       cohortTable = cohortTable)
  readr::write_csv(allExposureCohorts, file.path(outputFolder, "AllExposureCohorts.csv"))

  ParallelLogger::logInfo("Creating negative control outcome cohorts")
  negativeControls <- loadNegativeControls()
  sql <- SqlRender::loadRenderTranslateSql("NegativeControlOutcomes.sql",
                                           "Eumaeus",
                                           dbms = connectionDetails$dbms,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable,
                                           outcome_ids = unique(negativeControls$outcomeId))
  DatabaseConnector::executeSql(connection, sql)
  
  # Check number of subjects per cohort:
  ParallelLogger::logInfo("Counting cohorts")
  sql <- "SELECT cohort_definition_id, 
    COUNT(*) AS entry_count, 
    COUNT(DISTINCT subject_id) AS subject_count 
  FROM @cohort_database_schema.@cohort_table 
  GROUP BY cohort_definition_id"
  sql <- SqlRender::render(sql,
                           cohort_database_schema = cohortDatabaseSchema,
                           cohort_table = cohortTable)
  sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"))
  counts <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
  counts <- addCohortNames(counts, outputFolder = outputFolder)
  readr::write_csv(x = counts, file = file.path(outputFolder, "CohortCounts.csv"))
}

deriveExposureCohorts <- function(row, connection, cohortDatabaseSchema, cohortTable) {
  if (row$shots == 1) {
    row$baseExposureId <- row$exposureId
    row$baseExposureName <- row$exposureName
    row$shot <- "First"
    return(row)
  } else {
    ParallelLogger::logInfo(paste("Deriving exposure cohorts for:", row$exposureName))
    firstRow <- row
    firstRow$baseExposureId <- row$exposureId
    firstRow$baseExposureName <- row$exposureName
    firstRow$exposureId <- row$exposureId * 10 + 1
    firstRow$exposureName <- paste("First", row$exposureName)
    firstRow$shot <- "First"
    
    secondRow <- row
    secondRow$baseExposureId <- row$exposureId
    secondRow$baseExposureName <- row$exposureName
    secondRow$exposureId <- row$exposureId * 10 + 2
    secondRow$exposureName <- paste("Second", row$exposureName)
    secondRow$shot <- "Second"
    
    bothRow <- row
    bothRow$baseExposureId <- row$exposureId
    bothRow$baseExposureName <- row$exposureName
    bothRow$exposureId <- row$exposureId * 10 + 3
    bothRow$exposureName <- paste("First or second", row$exposureName)
    bothRow$shot <- "Both"
    
    sql <- SqlRender::loadRenderTranslateSql("DeriveExposureCohorts.sql",
                                             "Eumaeus",
                                             dbms = connectionDetails$dbms,
                                             cohort_database_schema = cohortDatabaseSchema,
                                             cohort_table = cohortTable,
                                             exposure_id = row$exposureId,
                                             first_exposure_id = firstRow$exposureId,
                                             second_exposure_id = secondRow$exposureId,
                                             both_exposure_id = bothRow$exposureId)
    DatabaseConnector::executeSql(connection, sql)
    
    return(bind_rows(firstRow, secondRow, bothRow))
  }
}

sampleComparatorCohorts <- function(row,
                                    connection,
                                    cdmDatabaseSchema,
                                    cohortDatabaseSchema,
                                    cohortTable) {
  ParallelLogger::logInfo(paste("Sampling comparator cohorts for:", row$exposureName))
  
  originalRow <- row
  originalRow$sampled <- FALSE
  originalRow$comparator <- FALSE
  originalRow$comparatorType <- NA
  
  sampleTargetRow <- row
  sampleTargetRow$sampled <- TRUE
  sampleTargetRow$comparator <- FALSE
  sampleTargetRow$exposureId <- row$exposureId * 10 + 1
  sampleTargetRow$exposureName <- paste("Sampled", row$exposureName)
  sampleTargetRow$comparatorType <- NA
  
  sampleComparatorRow <- row
  sampleComparatorRow$sampled <- TRUE
  sampleComparatorRow$comparator <- TRUE
  sampleComparatorRow$exposureId <- row$exposureId * 10 + 2
  sampleComparatorRow$exposureName <- paste("Age-sex stratified comparator for", row$exposureName)
  sampleComparatorRow$comparatorType <- "Age-sex stratified"
  
  sampleCrudeComparatorRow <- row
  sampleCrudeComparatorRow$sampled <- TRUE
  sampleCrudeComparatorRow$comparator <- TRUE
  sampleCrudeComparatorRow$exposureId <- row$exposureId * 10 + 3
  sampleCrudeComparatorRow$exposureName <- paste("Crude comparator for", row$exposureName)
  sampleCrudeComparatorRow$comparatorType <- "Crude"
  
  sql <- SqlRender::loadRenderTranslateSql("SampleComparators.sql",
                                           "Eumaeus",
                                           dbms = connectionDetails$dbms,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable,
                                           exposure_id = row$exposureId,
                                           target_sample_cohort_id = sampleTargetRow$exposureId,
                                           comparator_sample_cohort_id = sampleComparatorRow$exposureId,
                                           crude_comparator_sample_cohort_id = sampleCrudeComparatorRow$exposureId,
                                           exclusion_cohort_id = row$exclusionCohortId,
                                           start_date = format(row$startDate, "%Y%m%d"),
                                           end_date = format(row$endDate, "%Y%m%d"),
                                           washout_period = 365,
                                           multiplier = row$comparatorMultiplier,
                                           max_target_per_month = 350000,
                                           visit_concept_ids = c(9202))
  
  DatabaseConnector::executeSql(connection, sql)
  
  return(bind_rows(originalRow, sampleTargetRow, sampleComparatorRow, sampleCrudeComparatorRow))
}
