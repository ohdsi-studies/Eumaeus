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

#' Execute the Feasibility assessment
#'
#' @details
#' This function executes the Study.
#'
#' The \code{createCohorts}, \code{synthesizePositiveControls}, \code{runAnalyses}, and \code{runDiagnostics} arguments
#' are intended to be used to run parts of the full study at a time, but none of the parts are considerd to be optional.
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
#' @param minCellCount          The minimum cell count for fields contains person counts or fractions.
#' @param databaseId            A short string for identifying the database (e.g. 'Synpuf').
#' @param databaseName          The full name of the database.
#' @param databaseDescription   A short description (several sentences) of the database.
#' @param createCohorts        Create the exposure and outcome?
#' @param runCohortDiagnostics Run cohort diagnostics?
#'
#' @export
runCohortDiagnostics <- function(connectionDetails,
                                 cdmDatabaseSchema,
                                 cohortDatabaseSchema,
                                 cohortTable,
                                 outputFolder,
                                 minCellCount,
                                 databaseId,
                                 databaseName = databaseId,
                                 databaseDescription = databaseId,
                                 createCohorts = TRUE,
                                 runCohortDiagnostics = TRUE) {
    cohortDiagnosticsFolder <- file.path(outputFolder, "cohortDiagnostics")
    if (!file.exists(cohortDiagnosticsFolder)) {
        dir.create(cohortDiagnosticsFolder, recursive = TRUE)
    }
    
    ParallelLogger::addDefaultFileLogger(file.path(cohortDiagnosticsFolder, "log.txt"))
    ParallelLogger::addDefaultErrorReportLogger(file.path(cohortDiagnosticsFolder, "errorReportR.txt"))
    on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
    on.exit(ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE), add = TRUE)
    
    if (createCohorts) {
        ParallelLogger::logInfo("Creating base exposure cohorts")
        connection <- DatabaseConnector::connect(connectionDetails)
        .createCohorts(connection = connection,
                                 cdmDatabaseSchema = cdmDatabaseSchema,
                                 cohortDatabaseSchema = cohortDatabaseSchema,
                                 cohortTable = cohortTable,
                                 outputFolder = outputFolder)
        DatabaseConnector::disconnect(connection)
    }
    
    if (runCohortDiagnostics) {
        ParallelLogger::logInfo("Running cohort diagnostics")
        CohortDiagnostics::runCohortDiagnostics(packageName = "Eumaeus",
                                                cohortToCreateFile = "settings/CohortsToCreate.csv",
                                                connectionDetails = connectionDetails,
                                                cdmDatabaseSchema = cdmDatabaseSchema,
                                                cohortDatabaseSchema = cohortDatabaseSchema,
                                                cohortTable = cohortTable,
                                                exportFolder = cohortDiagnosticsFolder,
                                                minCellCount = minCellCount,
                                                databaseId = databaseId,
                                                databaseName = databaseName,
                                                databaseDescription = databaseDescription,
                                                runInclusionStatistics = FALSE,
                                                runBreakdownIndexEvents = TRUE,
                                                runCohortCharacterization = TRUE,
                                                runIncludedSourceConcepts = FALSE,
                                                runCohortOverlap = FALSE,
                                                runIncidenceRate = TRUE,
                                                runOrphanConcepts = FALSE,
                                                runTemporalCohortCharacterization = TRUE,
                                                runTimeDistributions = FALSE,
                                                runVisitContext = FALSE)
    }
}
