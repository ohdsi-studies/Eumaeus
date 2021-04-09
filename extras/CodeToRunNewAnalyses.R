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

unlink(file.path(outputFolder, "sccsSummary.csv"))
toDelete <- list.files(file.path(outputFolder, "sccs"), "estimates_t[0-9]+.csv", recursive = TRUE, full.names = TRUE)
unlink(toDelete)
unlink(file.path(outputFolder, "cmSummary.csv"))
toDelete <- list.files(file.path(outputFolder, "cohortMethod"), "estimates_t[0-9]+.csv", recursive = TRUE, full.names = TRUE)
unlink(toDelete)
toDelete <- list.files(file.path(outputFolder, "cohortMethod"), "Analysis_(19|20|21|22|23|24)$", recursive = TRUE, full.names = TRUE, include.dirs = TRUE)
unlink(toDelete)

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
        runSccs = TRUE,
        runCaseControl = F,
        runHistoricalComparator = TRUE,
        generateDiagnostics = F,
        computeCriticalValues = TRUE,
        createDbCharacterization = F,
        exportResults = TRUE)

uploadResults(outputFolder = outputFolder,
              privateKeyFileName = "c:/home/keyfiles/study-data-site-covid19.dat",
              userName = "study-data-site-covid19")
