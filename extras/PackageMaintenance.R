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

# Format and check code
OhdsiRTools::formatRFolder()
OhdsiRTools::checkUsagePackage("Eumaeus")
OhdsiRTools::updateCopyrightYearFolder()

# Insert cohort definitions into package
ROhdsiWebApi::insertCohortDefinitionSetInPackage(fileName = "inst/settings/CohortsToCreate.csv",
                                                 baseUrl = Sys.getenv("baseUrl"),
                                                 insertTableSql = TRUE,
                                                 insertCohortCreationR = TRUE,
                                                 generateStats = FALSE,
                                                 packageName = "Eumaeus")


# Create analysis details
Eumaeus::createCohortMethodSettings(fileName = "inst/settings/cmAnalysisSettings.txt")
Eumaeus::createSccsSettings(fileName = "inst/settings/sccsAnalysisSettings.txt")
Eumaeus::createSelfControlledCohortSettings(fileName = "inst/settings/sccAnalysisSettings.txt")



