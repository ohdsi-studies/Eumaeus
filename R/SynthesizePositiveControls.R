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

synthesizePositiveControls <- function(connectionDetails,
                                       cdmDatabaseSchema,
                                       cohortDatabaseSchema,
                                       cohortTable,
                                       outputFolder,
                                       maxCores) {
  
  synthesisFolder <- file.path(outputFolder, "positiveControlSynthesis")
  if (!file.exists(synthesisFolder)) {
    dir.create(synthesisFolder)
  }
  
  synthesisSummaryFile <- file.path(outputFolder, "SynthesisSummary.csv")
  if (!file.exists(synthesisSummaryFile)) {
    exposureOutcomePairs <- loadNegativeControls() %>%
      distinct(.data$outcomeId) %>%
      inner_join(loadExposuresofInterest() %>% distinct(.data$exposureId), by = character(0))
    
    prior <- Cyclops::createPrior("laplace", exclude = 0, useCrossValidation = TRUE)
    
    control <- Cyclops::createControl(cvType = "auto",
                                      startingVariance = 0.01,
                                      noiseLevel = "quiet",
                                      cvRepetitions = 1,
                                      threads = min(c(10, maxCores)))
    
    covariateSettings <- FeatureExtraction::createCovariateSettings(useDemographicsAgeGroup = TRUE,
                                                                    useDemographicsGender = TRUE,
                                                                    useDemographicsIndexYear = TRUE,
                                                                    useDemographicsIndexMonth = TRUE,
                                                                    useConditionGroupEraLongTerm = TRUE,
                                                                    useDrugGroupEraLongTerm = TRUE,
                                                                    useProcedureOccurrenceLongTerm = TRUE,
                                                                    useMeasurementLongTerm = TRUE,
                                                                    useObservationLongTerm = TRUE,
                                                                    useCharlsonIndex = TRUE,
                                                                    useDcsi = TRUE,
                                                                    useChads2Vasc = TRUE,
                                                                    longTermStartDays = 365,
                                                                    endDays = 0)
    
    result <- MethodEvaluation::synthesizePositiveControls(connectionDetails = connectionDetails,
                                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                                           exposureDatabaseSchema = cohortDatabaseSchema,
                                                           exposureTable = cohortTable,
                                                           outcomeDatabaseSchema = cohortDatabaseSchema,
                                                           outcomeTable = cohortTable,
                                                           outputDatabaseSchema = cohortDatabaseSchema,
                                                           outputTable = cohortTable,
                                                           createOutputTable = FALSE,
                                                           exposureOutcomePairs = exposureOutcomePairs,
                                                           workFolder = synthesisFolder,
                                                           modelThreads = max(1, round(maxCores/8)),
                                                           generationThreads = min(6, maxCores),
                                                           outputIdOffset = 10000,
                                                           firstExposureOnly = FALSE,
                                                           firstOutcomeOnly = TRUE,
                                                           removePeopleWithPriorOutcomes = TRUE,
                                                           modelType = "survival",
                                                           washoutPeriod = 365,
                                                           riskWindowStart = 0,
                                                           riskWindowEnd = 30,
                                                           addIntentToTreat = FALSE,
                                                           endAnchor = "cohort start",
                                                           effectSizes = c(1.5, 2, 4),
                                                           precision = 0.01,
                                                           prior = prior,
                                                           control = control,
                                                           maxSubjectsForModel = 250000,
                                                           minOutcomeCountForModel = 100,
                                                           minOutcomeCountForInjection = 25,
                                                           covariateSettings = covariateSettings)
    readr::write_csv(result, synthesisSummaryFile)
  } 
  ParallelLogger::logTrace("Merging positive with negative controls ")
  negativeControls <- loadNegativeControls() %>%
    distinct(.data$outcomeId, .data$outcomeName) %>%
    inner_join(loadExposuresofInterest() %>% distinct(.data$exposureId), by = character(0))
  
  synthesisSummary <- loadSynthesisSummary(synthesisSummaryFile)
  synthesisSummary$targetId <- synthesisSummary$exposureId
  synthesisSummary <- synthesisSummary %>%
    inner_join(negativeControls, by = c("exposureId", "outcomeId")) %>%
    mutate(outcomeName = paste0(.data$outcomeName, ", RR=", .data$targetEffectSize),
           oldOutcomeId = .data$outcomeId) %>%
    mutate(outcomeId = .data$newOutcomeId)
  
  negativeControls$targetEffectSize <- 1
  negativeControls$trueEffectSize <- 1
  negativeControls$trueEffectSizeFirstExposure <- 1
  negativeControls$oldOutcomeId <- negativeControls$outcomeId
  allControls <- bind_rows(negativeControls, synthesisSummary[, names(negativeControls)])
  
  exposuresOfInterest <- loadExposuresofInterest()
  allControls <- allControls %>%
    inner_join(exposuresOfInterest %>% select(-.data$exposureName ), by = "exposureId")
  
  readr::write_csv(allControls, file.path(outputFolder, "AllControls.csv"))
}