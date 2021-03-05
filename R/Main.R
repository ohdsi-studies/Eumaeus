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

#' Execute the Study
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
#' @param outcomeDatabaseSchema Schema name where outcome data can be stored. You will need to have
#'                             write priviliges in this schema. Note that for SQL Server, this should
#'                             include both the database and schema name, for example 'cdm_data.dbo'.
#' @param outcomeTable          The name of the table that will be created in the outcomeDatabaseSchema.
#'                             This table will hold the outcome cohorts used in this
#'                             study.
#' @param exposureDatabaseSchema For PanTher only: Schema name where exposure data can be stored. You will need to have
#'                             write priviliges in this schema. Note that for SQL Server, this should
#'                             include both the database and schema name, for example 'cdm_data.dbo'.
#' @param exposureTable          For PanTher only: The name of the table that will be created in the exposureDatabaseSchema
#'                             This table will hold the exposure cohorts used in this
#'                             study.
#' @param nestingCohortDatabaseSchema Schema name where nesting cohort data can be stored. You will need to have
#'                             write priviliges in this schema. Note that for SQL Server, this should
#'                             include both the database and schema name, for example 'cdm_data.dbo'.
#' @param nestingCohortTable          The name of the table that will be created in the nestingCohortDatabaseSchema
#'                             This table will hold the nesting cohorts used in this
#'                             study.
#' @param outputFolder         Name of local folder to place results; make sure to use forward slashes
#'                             (/). Do not use a folder on a network drive since this greatly impacts
#'                             performance.
#' @param databaseName         A short string for identifying the database (e.g.
#'                             'Synpuf').
#' @param maxCores             How many parallel cores should be used? If more cores are made available
#'                             this can speed up the analyses.
#' @param cdmVersion           Version of the Common Data Model used. Currently only version 5 is supported.
#' @param createCohorts        Create the exposure and outcome cohorts?
#' @param imputeExposureLengthForPanther      For PanTher only: impute exposure length?
#' @param synthesizePositiveControls          Should positive controls be synthesized?
#' @param runCohortMethod                     Perform the cohort method analyses?
#' @param runSelfControlledCaseSeries                     Perform the SCCS analyses?
#' @param runSelfControlledCohort                     Perform the SCC analyses?
#' @param runCaseControl                     Perform the case-control analyses?
#' @param runCaseCrossover       Perform the case-crossover analyses?
#' @param createCharacterization       Create a general characterization of the database population?
#' @param packageResults       Should results be packaged for later sharing and viewing?
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
                    maxCores = 1,
                    cdmVersion = "5",
                    createCohorts = TRUE,
                    synthesizePositiveControls = TRUE,
                    runCohortMethod = TRUE,
                    packageResults = TRUE) {
    if (!file.exists(outputFolder)) {
        dir.create(outputFolder, recursive = TRUE)
    }

    ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
    ParallelLogger::addDefaultErrorReportLogger(file.path(outputFolder, "errorReportR.txt"))
    on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
    on.exit(ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE), add = TRUE)

    if (createCohorts) {
        ParallelLogger::logInfo("Creating exposure and outcome cohorts")
        Eumaeus:::createCohorts(connectionDetails = connectionDetails,
                      cdmDatabaseSchema = cdmDatabaseSchema,
                      cohortDatabaseSchema = cohortDatabaseSchema,
                      cohortTable = cohortTable,
                      outputFolder = outputFolder)
       
    }

    if (synthesizePositiveControls) {
        ParallelLogger::logInfo("Synthesizing positive controls")
        Eumaeus:::synthesizePositiveControls(connectionDetails = connectionDetails,
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
                        oracleTempSchema = oracleTempSchema,
                        outcomeDatabaseSchema = outcomeDatabaseSchema,
                        outcomeTable = outcomeTable,
                        outputFolder = outputFolder,
                        cdmVersion = cdmVersion,
                        maxCores = maxCores)
    }

   

    if (packageResults) {
        ParallelLogger::logInfo("Packaging results")
        packageResults(outputFolder = outputFolder,
                       exportFolder = file.path(outputFolder, "export"),
                       databaseName = databaseName)
    }
}
