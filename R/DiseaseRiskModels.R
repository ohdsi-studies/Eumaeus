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


fitDiseaseRiskModels <- function(connectionDetails,
                                 cdmDatabaseSchema,
                                 oracleTempSchema = NULL,
                                 outcomeDatabaseSchema,
                                 outcomeTable,
                                 nestingCohortDatabaseSchema,
                                 nestingCohortTable,
                                 outputFolder,
                                 maxCores = 1,
                                 cdmVersion = "5") {
    start <- Sys.time()

    diseaseRiskFolder <- file.path(outputFolder, "diseaseRiskModels")
    if (!file.exists(diseaseRiskFolder))
        dir.create(diseaseRiskFolder)

    ParallelLogger::addDefaultErrorReportLogger(file.path(diseaseRiskFolder, "errorReportR.txt"))
    ParallelLogger::addDefaultFileLogger(file.path(diseaseRiskFolder, "log.txt"))

    allControls <- read.csv(file.path(outputFolder , "allControls.csv"))

    createPlpData <- function(nestingControlSet,
                              diseaseRiskFolder,
                              connectionDetails,
                              cdmDatabaseSchema,
                              cdmVersion,
                              oracleTempSchema,
                              outcomeDatabaseSchema,
                              outcomeTable,
                              nestingCohortDatabaseSchema,
                              nestingCohortTable) {
        fileName <- file.path(diseaseRiskFolder, sprintf("plpData_n%s", nestingControlSet$nestingId[1]))
        if (!file.exists(fileName)) {
            ParallelLogger::logInfo("Creating PlpData for nesting ID ", nestingControlSet$nestingId[1])
            exposureConceptIds <- unique(c(nestingControlSet$targetId, nestingControlSet$comparatorId))
            covarSettings <- FeatureExtraction::createDefaultCovariateSettings(excludedCovariateConceptIds = exposureConceptIds,
                                                                               addDescendantsToExclude = TRUE)

            plpData <- PatientLevelPrediction::getPlpData(connectionDetails = connectionDetails,
                                                          cdmDatabaseSchema = cdmDatabaseSchema,
                                                          cdmVersion = cdmVersion,
                                                          oracleTempSchema = oracleTempSchema,
                                                          cohortId = nestingControlSet$nestingId[1],
                                                          outcomeIds = nestingControlSet$outcomeId,
                                                          cohortDatabaseSchema = nestingCohortDatabaseSchema,
                                                          cohortTable = nestingCohortTable,
                                                          outcomeDatabaseSchema = outcomeDatabaseSchema,
                                                          outcomeTable = outcomeTable,
                                                          washoutPeriod = 365,
                                                          sampleSize = 1e6,
                                                          covariateSettings = covarSettings)
            PatientLevelPrediction::savePlpData(plpData, fileName)
        }
    }
    cluster <- ParallelLogger::makeCluster(min(3, maxCores))
    nestingControlSets <- split(allControls, allControls$nestingId)
    ParallelLogger::clusterApply(cluster = cluster,
                                 x = nestingControlSets,
                                 fun = createPlpData,
                                 diseaseRiskFolder = diseaseRiskFolder,
                                 connectionDetails = connectionDetails,
                                 cdmDatabaseSchema = cdmDatabaseSchema,
                                 cdmVersion = cdmVersion,
                                 oracleTempSchema = oracleTempSchema,
                                 outcomeDatabaseSchema = outcomeDatabaseSchema,
                                 outcomeTable = outcomeTable,
                                 nestingCohortDatabaseSchema = nestingCohortDatabaseSchema,
                                 nestingCohortTable = nestingCohortTable)
    ParallelLogger::stopCluster(cluster)

    fitDiseaseRiskModel <- function(outcomeNestingPair, diseaseRiskFolder) {
        fileName <- file.path(diseaseRiskFolder, sprintf("drBetas_n%o_o%s.rds", outcomeNestingPair$nestingId, outcomeNestingPair$outcomeId))
        if (!file.exists(fileName)) {
            ParallelLogger::logInfo("Fitting model for nesting ID ", outcomeNestingPair$nestingId, " and outcome ID ", outcomeNestingPair$outcomeId)
            plpFileName <- file.path(diseaseRiskFolder, sprintf("plpData_n%s", outcomeNestingPair$nestingId))
            plpData <- PatientLevelPrediction::loadPlpData(plpFileName)
            plpStudyPop <- PatientLevelPrediction::createStudyPopulation(plpData = plpData,
                                                                         outcomeId = outcomeNestingPair$outcomeId,
                                                                         includeAllOutcomes = TRUE,
                                                                         removeSubjectsWithPriorOutcome = TRUE,
                                                                         minTimeAtRisk = 1,
                                                                         riskWindowStart = 1,
                                                                         riskWindowEnd = 60)
            model <- PatientLevelPrediction::fitGLMModel(population = plpStudyPop,
                                                         plpData = plpData,
                                                         modelType = "logistic",
                                                         control = Cyclops::createControl(cvType = "auto",
                                                                                          fold = 3,
                                                                                          startingVariance = 0.01,
                                                                                          tolerance  = 2e-06,
                                                                                          cvRepetitions = 1,
                                                                                          selectorType = "byPid",
                                                                                          noiseLevel = "silent",
                                                                                          threads = 3,
                                                                                          maxIterations = 3000))

            betas <- model$coefficients
            betas <- betas[betas != 0]
            saveRDS(betas, fileName)
        }
    }
    cluster <- ParallelLogger::makeCluster(max(3, min(1, maxCores / 3)))
    outcomeNestingPairs <- unique(allControls[, c("nestingId", "outcomeId")])

    ParallelLogger::clusterApply(cluster = cluster,
                                 x = split(outcomeNestingPairs, 1:nrow(outcomeNestingPairs)),
                                 fun = fitDiseaseRiskModel,
                                 diseaseRiskFolder = diseaseRiskFolder)
    ParallelLogger::stopCluster(cluster)

    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste("Completed CohortMethod analyses in", signif(delta, 3), attr(delta, "units")))
}
