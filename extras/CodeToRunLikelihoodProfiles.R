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

# Code to rerun the analyses to produce the likelihood profiles for SCCS, CaseControl, and HistoricalComparator


remove.packages("Cyclops")
renv::purge("Cyclops", version = "3.1.1", prompt = FALSE)

remove.packages("CohortMethod")
renv::purge("CohortMethod", version = "4.2.0", prompt = FALSE)

remove.packages("CaseControl")
renv::purge("CaseControl", version = "3.2.0", prompt = FALSE)

remove.packages("SelfControlledCaseSeries")
renv::purge("SelfControlledCaseSeries", version = "3.1.0", prompt = FALSE)

remove.packages("EmpiricalCalibration")
renv::purge("EmpiricalCalibration", version = "2.2.0", prompt = FALSE)

renv::restore()

unlink(file.path(outputFolder, "sccsSummary.csv"))
toDelete <- list.files(file.path(outputFolder, "sccs"), "estimates_t[0-9]+.csv", recursive = TRUE, full.names = TRUE)
unlink(toDelete)
toDelete <- list.files(file.path(outputFolder, "sccs"), "SccsModel_e[0-9]+_o[0-9]+.rds", recursive = TRUE, full.names = TRUE)
unlink(toDelete)
toModify <- list.files(file.path(outputFolder, "sccs"), "SccsIntervalData_e[0-9]+_o[0-9]+.zip", recursive = TRUE, full.names = TRUE)
requireProfile <- function(fileName) {
        x <<- fileName
        sccsIntervalData <- SelfControlledCaseSeries::loadSccsIntervalData(fileName)    
        metaData <- attr(sccsIntervalData, "metaData")
        if ("covariateSettingsList" %in% names(metaData)) {
                for (i in 1:length(metaData$covariateSettingsList)) {
                        if (metaData$covariateSettingsList[[i]]$label %in% c("Main", "Second")) {
                                metaData$covariateSettingsList[[i]]$profileLikelihood <- TRUE    
                        }
                }
                attr(sccsIntervalData, "metaData") <- metaData
                suppressMessages(
                        SelfControlledCaseSeries::saveSccsIntervalData(sccsIntervalData, fileName)
                )
        }
        return(NULL)
}
plyr::l_ply(toModify, requireProfile, .progress = "text")

unlink(file.path(outputFolder, "ccSummary.csv"))
toDelete <- list.files(file.path(outputFolder, "caseControl"), "estimates_t[0-9]+.csv", recursive = TRUE, full.names = TRUE)
unlink(toDelete)
toDelete <- list.files(file.path(outputFolder, "caseControl"), "Analysis_[0-9]+$", recursive = TRUE, full.names = TRUE, include.dirs = TRUE)
unlink(toDelete, recursive = TRUE)

unlink(file.path(outputFolder, "export", "estimate.csv"))



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
        runCohortMethod = F,
        runSccs = TRUE,
        runCaseControl = TRUE,
        runHistoricalComparator = F,
        generateDiagnostics = F,
        computeCriticalValues = TRUE,
        createDbCharacterization = F,
        exportResults = TRUE)

uploadResults(outputFolder = outputFolder,
              privateKeyFileName = "c:/home/keyfiles/study-data-site-covid19.dat",
              userName = "study-data-site-covid19")
