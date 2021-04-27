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

# Check if CDM version hasn't changed --------------------
source("extras/GetDatabaseVersion.R")
version <- getDatabaseVersion(connectionDetails, cdmDatabaseSchema)
read.csv(file.path(outputFolder, "version.csv"))

library(dplyr)
cohortsToResample <- Eumaeus:::loadExposureCohorts(outputFolder) %>%
  filter(.data$sampled == TRUE & .data$comparator == TRUE & .data$comparatorType == "Random day age-sex stratified")

# Delete erroneous data ------------------------------------------------
unlink(file.path(outputFolder, "cmSummary.csv"))
for (cohortId in cohortsToResample$exposureId) {
  toDelete <- list.files(file.path(outputFolder, "cohortMethod"), sprintf("_c%s$", cohortId), include.dirs = , full.names = TRUE)
  unlink(toDelete, recursive = TRUE)
}

# Resample comparator cohort ----------------------------------------------
connection <- DatabaseConnector::connect(connectionDetails)
for (i in 1:nrow(cohortsToResample)) {
  row <- cohortsToResample[i, ]
  ParallelLogger::logInfo("Resampling ", row$exposureName)
  targetSampleCohortId <- row$baseExposureId * 10 + 1
  sql <- SqlRender::readSql("extras/ResampleStratifiedRandomDay.sql")
  DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                               sql = sql,
                                               cdm_database_schema = cdmDatabaseSchema,
                                               cohort_database_schema = cohortDatabaseSchema,
                                               cohort_table = cohortTable,
                                               target_sample_cohort_id = targetSampleCohortId,
                                               random_date_comparator_sample_cohort_id = row$exposureId,
                                               exclusion_cohort_id = row$exclusionCohortId,
                                               start_date = format(row$startDate, "%Y%m%d"),
                                               end_date = format(row$endDate, "%Y%m%d"),
                                               washout_period = 365,
                                               multiplier = row$comparatorMultiplier)
}

# Check number of subjects per cohort -------------------------------
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
counts <- Eumaeus:::addCohortNames(counts, outputFolder = outputFolder)
readr::write_csv(x = counts, file = file.path(outputFolder, "CohortCountsNew.csv"))

DatabaseConnector::disconnect(connection)

# Rerun analyses ----------------------------------------
execute(connectionDetails = connectionDetails,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        databaseId = databaseId,
        databaseName = databaseName,
        databaseDescription = databaseDescription,
        outputFolder = outputFolder,
        maxCores = maxCores,
        exposureIds = getExposuresOfInterest()$exposureId,
        verifyDependencies = TRUE,
        createCohorts = F,
        synthesizePositiveControls = F,
        runCohortMethod = TRUE,
        runSccs = F,
        runCaseControl = F,
        runHistoricalComparator = F,
        generateDiagnostics = F,
        computeCriticalValues = TRUE,
        createDbCharacterization = F,
        exportResults = TRUE)

uploadResults(outputFolder = outputFolder,
              privateKeyFileName = "c:/home/keyfiles/study-data-site-covid19.dat",
              userName = "study-data-site-covid19")
