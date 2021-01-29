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

createCohorts <- function(connectionDetails,
                          cdmDatabaseSchema,
                          cohortDatabaseSchema,
                          cohortTable,
                          outputFolder) {
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  .createCohorts(connection = connection,
                 cdmDatabaseSchema = cdmDatabaseSchema,
                 cohortDatabaseSchema = cohortDatabaseSchema,
                 cohortTable = cohortTable,
                 outputFolder = outputFolder)
  
  negativeControls <- loadNegativeControls()
  
  ParallelLogger::logInfo("Creating negative control outcome cohorts")
  sql <- SqlRender::loadRenderTranslateSql("NegativeControlOutcomes.sql",
                                           "VaccineSurveillanceMethodEvaluation",
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
  counts <- addCohortNames(counts)
  readr::write_csv(x = counts, file = file.path(outputFolder, "CohortCounts.csv"))
}
