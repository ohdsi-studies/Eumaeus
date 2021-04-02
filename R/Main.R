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

#' Get the exposures of interest
#'
#' @return
#' A tibble with exposure IDs and names.
#' 
#' @export
getExposuresOfInterest <- function() {
  loadExposuresofInterest() %>% 
    select(.data$exposureId,
           .data$exposureName) %>%
    return()
}

#' Execute the Study
#'
#' @details
#' This function executes the Study.
#'
#' @param connectionDetails    An object of type \code{connectionDetails} as created using the
#'                             \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                             DatabaseConnector package.
#' @param cdmDatabaseSchema    Schema name where your patient-level data in OMOP CDM format resides.
#'                             Note that for SQL Server, this should include both the database and
#'                             schema name, for example 'cdm_data.dbo'.
#' @param cohortDatabaseSchema Schema name where outcome data can be stored. You will need to have
#'                             write priviliges in this schema. Note that for SQL Server, this should
#'                             include both the database and schema name, for example 'cdm_data.dbo'.
#' @param cohortTable          The name of the table that will be created in the \code{cohortDatabaseSchema}.
#'                             This table will hold the exposure and outcome cohorts used in this
#'                             study.
#' @param outputFolder         Name of local folder to place results; make sure to use forward slashes
#'                             (/). Do not use a folder on a network drive since this greatly impacts
#'                             performance.
#' @param databaseId            A short string for identifying the database (e.g. 'Synpuf').
#' @param databaseName          The full name of the database.
#' @param databaseDescription   A short description (several sentences) of the database.
#' @param minCellCount          The minimum cell count for fields contains person counts or fractions.
#' @param maxCores             How many parallel cores should be used? If more cores are made available
#'                             this can speed up the analyses.
#' @param exposureIds          The IDs of the exposure cohorts to include. See \code{\link{getExposuresOfInterest}}
#'                             for the available exposure cohorts and their IDs.
#' @param verifyDependencies   Check whether correct package versions are installed?
#' @param createCohorts        Create the exposure and outcome cohorts?
#' @param synthesizePositiveControls          Should positive controls be synthesized?
#' @param runCohortMethod                     Perform the cohort method analyses?
#' @param runSccs                     Perform the SCCS and SCRI analyses?
#' @param runCaseControl                     Perform the case-control analyses?
#' @param runHistoricalComparator       Perform the historical comparator analyses?
#' @param generateDiagnostics   Generate additional study diagnostics?
#' @param computeCriticalValues Compute critical values for all methods?
#' @param createDbCharacterization  Create a high-level characterization of the database?
#' @param exportResults       Export the results to a single zip file (containing several CSV files) for sharing?
#'
#' @export
execute <- function(connectionDetails,
                    cdmDatabaseSchema,
                    cohortDatabaseSchema,
                    cohortTable,
                    outputFolder,
                    databaseId,
                    databaseName = databaseId,
                    databaseDescription = databaseId,
                    minCellCount = 5,
                    maxCores = 1,
                    exposureIds = getExposuresOfInterest()$exposureId,
                    verifyDependencies = TRUE,
                    createCohorts = TRUE,
                    synthesizePositiveControls = TRUE,
                    runCohortMethod = TRUE,
                    runSccs = TRUE,
                    runCaseControl = TRUE,
                    runHistoricalComparator = TRUE,
                    generateDiagnostics = TRUE,
                    computeCriticalValues = TRUE,
                    createDbCharacterization = TRUE,
                    exportResults = TRUE) {
  if (!file.exists(outputFolder)) {
    dir.create(outputFolder, recursive = TRUE)
  }
  
  ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
  ParallelLogger::addDefaultErrorReportLogger(file.path(outputFolder, "errorReportR.txt"))
  on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
  on.exit(ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE), add = TRUE)
  
  if (verifyDependencies) {
    ParallelLogger::logInfo("Checking whether correct package versions are installed")
    verifyDependencies()
  }
  
  if (createCohorts) {
    ParallelLogger::logInfo("Creating exposure and outcome cohorts")
    createCohorts(connectionDetails = connectionDetails,
                  cdmDatabaseSchema = cdmDatabaseSchema,
                  cohortDatabaseSchema = cohortDatabaseSchema,
                  cohortTable = cohortTable,
                  outputFolder = outputFolder,
                  exposureIds = exposureIds)
    
  }
  
  if (synthesizePositiveControls) {
    ParallelLogger::logInfo("Synthesizing positive controls")
    synthesizePositiveControls(connectionDetails = connectionDetails,
                               cdmDatabaseSchema = cdmDatabaseSchema,
                               cohortDatabaseSchema = cohortDatabaseSchema,
                               cohortTable = cohortTable,
                               outputFolder = outputFolder,
                               maxCores = maxCores)
  }
  
  if (runCohortMethod) {
    ParallelLogger::logInfo("Running CohortMethod")
    runCohortMethod(connectionDetails = connectionDetails,
                    cdmDatabaseSchema = cdmDatabaseSchema,
                    cohortDatabaseSchema = cohortDatabaseSchema,
                    cohortTable = cohortTable,
                    outputFolder = outputFolder,
                    maxCores = maxCores)
  }
  
  if (runSccs) {
    ParallelLogger::logInfo("Running SelfControlledCaseSeries")
    runSccs(connectionDetails = connectionDetails,
            cdmDatabaseSchema = cdmDatabaseSchema,
            cohortDatabaseSchema = cohortDatabaseSchema,
            cohortTable = cohortTable,
            outputFolder = outputFolder,
            maxCores = maxCores)
  }
  
  if (runCaseControl) {
    ParallelLogger::logInfo("Running CaseControl")
    runCaseControl(connectionDetails = connectionDetails,
                   cdmDatabaseSchema = cdmDatabaseSchema,
                   cohortDatabaseSchema = cohortDatabaseSchema,
                   cohortTable = cohortTable,
                   outputFolder = outputFolder,
                   maxCores = maxCores)
  }
  
  if (runHistoricalComparator) {
    ParallelLogger::logInfo("Running HistoricalComparator")
    runHistoricalComparator(connectionDetails = connectionDetails,
                            cdmDatabaseSchema = cdmDatabaseSchema,
                            cohortDatabaseSchema = cohortDatabaseSchema,
                            cohortTable = cohortTable,
                            outputFolder = outputFolder,
                            maxCores = maxCores)
  }
  
  if (generateDiagnostics) {
    ParallelLogger::logInfo("Generating additional diagnostics")
    generateDiagnostics(connectionDetails = connectionDetails,
                        cdmDatabaseSchema = cdmDatabaseSchema,
                        cohortDatabaseSchema = cohortDatabaseSchema,
                        cohortTable = cohortTable,
                        outputFolder = outputFolder,
                        maxCores = maxCores)
  }
  
  if (computeCriticalValues) {
    ParallelLogger::logInfo("Computing critical values")
    computeCriticalValues(outputFolder = outputFolder,
                          maxCores = maxCores)
  }
  
  if (createDbCharacterization) {
    ParallelLogger::logInfo("Creating database characterization")
    createDbCharacterization(connectionDetails = connectionDetails,
                             cdmDatabaseSchema = cdmDatabaseSchema,
                             outputFolder = outputFolder)
  }
  
  if (exportResults) {
    ParallelLogger::logInfo("Packaging results")
    exportResults(outputFolder = outputFolder,
                  connectionDetails = connectionDetails,
                  cdmDatabaseSchema = cdmDatabaseSchema,
                  databaseId = databaseId,
                  databaseName = databaseName,
                  databaseDescription = databaseDescription,
                  minCellCount = minCellCount,
                  maxCores = maxCores)
  }
  ParallelLogger::logFatal("Done")
}

verifyDependencies <- function() {
  expected <- RJSONIO::fromJSON("renv.lock")
  expected <- dplyr::bind_rows(expected[[2]])
  basePackages <- rownames(installed.packages(priority = "base"))
  expected <- expected[!expected$Package %in% basePackages, ]
  observedVersions <- sapply(sapply(expected$Package, packageVersion), paste, collapse = ".")
  expectedVersions <- sapply(sapply(expected$Version, numeric_version), paste, collapse = ".")
  mismatchIdx <- which(observedVersions != expectedVersions)
  if (length(mismatchIdx) > 0) {
    
    lines <- sapply(mismatchIdx, function(idx) sprintf("- Package %s version %s should be %s",
                                                       expected$Package[idx],
                                                       observedVersions[idx],
                                                       expectedVersions[idx]))
    message <- paste(c("Mismatch between required and installed package versions. Did you forget to run renv::restore()?",
                       lines),
                     collapse = "\n")
    stop(message)
  }
}
