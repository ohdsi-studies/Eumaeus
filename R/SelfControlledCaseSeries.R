# @file CohortMethod.R
#
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

#' @export
runSccs <- function(connectionDetails,
                    cdmDatabaseSchema,
                    cohortDatabaseSchema,
                    cohortTable,
                    outputFolder,
                    maxCores) {
  start <- Sys.time()
  
  sccsFolder <- file.path(outputFolder, "sccs")
  if (!file.exists(sccsFolder))
    dir.create(sccsFolder)
  
  sccsSummaryFile <- file.path(outputFolder, "sccsSummary.csv")
  if (!file.exists(sccsSummaryFile)) {
    allControls <- loadAllControls(outputFolder)
    
    exposureCohorts <- loadExposureCohorts(outputFolder) %>%
      filter(.data$sampled == FALSE & .data$comparator == FALSE)
    
    baseExposureIds <- exposureCohorts %>%
      distinct(.data$baseExposureId) %>%
      pull()
    allEstimates <- list()
    # baseExposureId <- baseExposureIds[3]
    for (baseExposureCohortId in baseExposureCohortIds) {
      exposures <- exposureCohorts %>%
        filter(.data$baseExposureId == !!baseExposureId) 
      
      controls <- allControls %>%
        filter(.data$exposureId == baseExposureId)
      
      exposureFolder <- file.path(sccsFolder, sprintf("e_%s", baseExposureId))
      if (!file.exists(exposureFolder))
        dir.create(exposureFolder)
      
      # Create one big sccsData object ----------------------------
      bigSccsDataFile <- file.path(exposureFolder, "SccsData.zip")
      if (!file.exists(bigSccsDataFile)) {
        ParallelLogger::logInfo(sprintf("Constructing SccsData object for exposure %s", baseExposureId))
        
        # Including all other exposures: (expensive)
        # bigSccsData <- SelfControlledCaseSeries::getDbSccsData(connectionDetails = connectionDetails,
        #                                                     cdmDatabaseSchema = cdmDatabaseSchema,
        #                                                     outcomeDatabaseSchema = cohortDatabaseSchema,
        #                                                     outcomeTable = cohortTable,
        #                                                     outcomeIds = controls$outcomeId,
        #                                                     customCovariateDatabaseSchema = cohortDatabaseSchema,
        #                                                     customCovariateTable = cohortTable,
        #                                                     customCovariateIds = exposures$exposureId,
        #                                                     useCustomCovariates = TRUE,
        #                                                     exposureDatabaseSchema = cdmDatabaseSchema,
        #                                                     exposureTable = "drug_era",
        #                                                     exposureIds = NULL,
        #                                                     studyStartDate = format(controls$startDate[1], "%Y%m%d"),
        #                                                     studyEndDate = format(controls$endDate[1], "%Y%m%d"),
        #                                                     maxCasesPerOutcome = 250000)
        bigSccsData <- SelfControlledCaseSeries::getDbSccsData(connectionDetails = connectionDetails,
                                                               cdmDatabaseSchema = cdmDatabaseSchema,
                                                               outcomeDatabaseSchema = cohortDatabaseSchema,
                                                               outcomeTable = cohortTable,
                                                               outcomeIds = controls$outcomeId,
                                                               exposureDatabaseSchema = cohortDatabaseSchema,
                                                               exposureTable = cohortTable,
                                                               exposureIds = exposures$exposureId,
                                                               studyStartDate = format(controls$startDate[1], "%Y%m%d"),
                                                               studyEndDate = format(controls$endDate[1], "%Y%m%d"),
                                                               maxCasesPerOutcome = 250000)
        
        SelfControlledCaseSeries::saveSccsData(bigSccsData, bigSccsDataFile)
        
      } 
      bigSccsData <- NULL
      timePeriods <- splitTimePeriod(startDate = controls$startDate[1], endDate = controls$endDate[1])
      # i <- 12
      for (i in 1:nrow(timePeriods)) {
        periodEstimatesFile <- file.path(exposureFolder, sprintf("estimates_t%d.csv", timePeriods$seqId[i]))
        if (!file.exists(periodEstimatesFile)) {
          ParallelLogger::logInfo(sprintf("Executing SCCS for exposure %s and period: %s", baseExposureId, timePeriods$label[i]))
          periodFolder <- file.path(exposureFolder, sprintf("sccsOutput_t%d", timePeriods$seqId[i]))
          if (!file.exists(periodFolder))
            dir.create(periodFolder)
          
          sccsDataFile <- file.path(periodFolder, "SccsData_l1.zip")
          if (!file.exists(sccsDataFile)) {
            ParallelLogger::logInfo("- Subsetting SccsData to period")
            if (is.null(bigSccsData)) {
              bigSccsData <- SelfControlledCaseSeries::loadSccsData(bigSccsDataFile)
            }
            subsetSccsData(bigSccsData = bigSccsData,
                           endDate = timePeriods$endDate[i],
                           sccsDataFile = sccsDataFile)
            
          }
          estimates <- computeSccsEstimates(exposures = exposures,
                                            outcomeIds = controls$outcomeId,
                                            periodFolder = periodFolder,
                                            maxCores = maxCores)
          readr::write_csv(estimates, periodEstimatesFile)
        } else {
          estimates <- loadCmEstimates(periodEstimatesFile)
        }
        estimates$seqId <- timePeriods$seqId[i]
        estimates$period <- timePeriods$label[i]
        allEstimates[[length(allEstimates) + 1]] <- estimates
      }
      
      # Output last propensity model:
      modelFile <- file.path(exposureFolder, "PsModel.csv")
      if (!file.exists(modelFile)) {
        if (is.null(bigsccsData)) {
          bigsccsData <- CohortMethod::loadCohortMethodData(bigsccsDataFile)
        }
        omr <- readRDS(file.path(exposureFolder, sprintf("cmOutput_t%d", timePeriods$seqId[nrow(timePeriods)]), "outcomeModelReference.rds"))
        ps <- readRDS(file.path(exposureFolder, sprintf("cmOutput_t%d", timePeriods$seqId[nrow(timePeriods)]), omr$sharedPsFile[omr$sharedPsFile != ""][1]))
        # om <- readRDS(file.path(exposureFolder, sprintf("cmOutput_t%d", timePeriods$seqId[nrow(timePeriods)]), omr$outcomeModelFile[omr$sharedPsFile != ""][1]))
        
        model <- CohortMethod::getPsModel(ps, bigsccsData)
        # model <- getPsModel(ps, bigsccsData)
        readr::write_csv(model, modelFile)
      }
    }
    allEstimates <- bind_rows(allEstimates)  
    allEstimates <- allEstimates %>%
      filter(.data$eventsComparator > 0) %>%
      mutate(exposureId = bit64::as.integer64(.data$targetId/100),
             expectedOutcomes = .data$targetDays * (.data$eventsComparator / .data$comparatorDays)) %>%
      mutate(llr = llr(.data$eventsTarget, .data$expectedOutcomes))
    readr::write_csv(allEstimates, sccsSummaryFile)
  }
  delta <- Sys.time() - start
  writeLines(paste("Completed cohort method analyses in", signif(delta, 3), attr(delta, "units")))
  
  analysisDesc <- tibble(analysisId = c(1, 
                                        2),
                         description = c("Crude cohort method",
                                         "1-on-1 matching"))
  readr::write_csv(analysisDesc, file.path(outputFolder, "cmAnalysisDesc.csv"))
}

subsetSccsData <- function(bigSccsData,
                           endDate,
                           sccsDataFile) {
  sccsData <- Andromeda::andromeda()
  
  sccsData$cases <- bigSccsData$cases %>%
    collect() %>%
    mutate(startDate = as.Date(paste(.data$startYear, .data$startMonth, .data$startDay, sep = "-"), format = "%Y-%m-%d")) %>%
    filter(.data$startDate < !!endDate) %>%
    mutate(newObservationDays = as.double(!!endDate - .data$startDate)) %>%
    mutate(noninformativeEndCensor = ifelse(.data$newObservationDays < .data$observationDays, 1L, .data$noninformativeEndCensor)) %>%
    mutate(observationDays = ifelse(.data$newObservationDays < .data$observationDays, .data$newObservationDays, .data$observationDays)) %>%
    select(-.data$startDate, -.data$newObservationDays)
  
  sccsData$eraRef <- bigSccsData$eraRef
  sccsData$eras <- bigSccsData$eras
  sccsData$eras <- sccsData$eras %>%
    inner_join(select(sccsData$cases, .data$caseId, .data$observationDays), by = "caseId") %>%
    filter(.data$startDay < .data$observationDays) %>%
    select(-.data$observationDays)
  
  metaData <- attr(bigSccsData, "metaData")
  attr(sccsData, "metaData") <- metaData
  class(sccsData) <- class(bigSccsData)
  SelfControlledCaseSeries::saveSccsData(sccsData, sccsDataFile)
}

computeSccsEstimates <- function(exposures,
                                 outcomeIds,
                                 periodFolder,
                                 maxCores) {
  sccsAnalysisList <- createSccsAnalysesList()
  exposureOutcomeList <- list()
  for (outcomeId in outcomeIds) {
    if (nrow(exposures) == 1) {
      exposureOutcome <- createExposureOutcome(exposureId = exposures$exposureId,
                                               exposureId2 = -1,
                                               outcomeId = outcomeId)
      exposureOutcomeList[[length(exposureOutcomeList) + 1]] <- exposureOutcome
    } else {
      exposureOutcome <- createExposureOutcome(exposureId = exposures$exposureId[exposures$shot == "Both"],
                                               exposureId2 = -1,
                                               outcomeId = outcomeId)
      exposureOutcomeList[[length(exposureOutcomeList) + 1]] <- exposureOutcome
      
      exposureOutcome <- createExposureOutcome(exposureId = exposures$exposureId[exposures$shot == "First"],
                                               exposureId2 = exposures$exposureId[exposures$shot == "Second"],
                                               outcomeId = outcomeId)
      exposureOutcomeList[[length(exposureOutcomeList) + 1]] <- exposureOutcome
    }
  }
  
  sccsResult <- SelfControlledCaseSeries::runSccsAnalyses(connectionDetails = NULL,
                                                          cdmDatabaseSchema = NULL,
                                                          outputFolder = periodFolder,
                                                          sccsAnalysisList = sccsAnalysisList,
                                                          exposureOutcomeList = exposureOutcomeList,
                                                          getDbSccsDataThreads = 1,
                                                          createStudyPopulationThreads = min(6, maxCores),
                                                          createSccsIntervalDataThreads = min(6, maxCores),
                                                          fitSccsModelThreads = min(6, maxCores),
                                                          cvThreads = 4)
  
  estimates <- SelfControlledCaseSeries::summarizeSccsAnalyses(sccsResult, periodFolder)
  return(estimates)
}

createSccsAnalysesList <- function() {
  getDbSccsDataArgs <- SelfControlledCaseSeries::createGetDbSccsDataArgs()
  
  createStudyPopulationArgs <- createCreateStudyPopulationArgs(naivePeriod = 365,
                                                               firstOutcomeOnly = FALSE)
  
  covarExposureOfInt <- SelfControlledCaseSeries::createEraCovariateSettings(label = "Exposure of interest",
                                                                             includeEraIds = "exposureId",
                                                                             start = 1,
                                                                             startAnchor = "era start",
                                                                             end = 28,
                                                                             endAnchor = "era start")
  
  covarPreExp <- SelfControlledCaseSeries::createEraCovariateSettings(label = "Pre-exposure",
                                                                      includeEraIds = "exposureId",
                                                                      start = -30,
                                                                      end = -1,
                                                                      endAnchor = "era start")

  covarExposureOfInt2 <- SelfControlledCaseSeries::createEraCovariateSettings(label = "Second dose",
                                                                             includeEraIds = "exposureId2",
                                                                             start = 1,
                                                                             startAnchor = "era start",
                                                                             end = 28,
                                                                             endAnchor = "era start")
  
  covarPreExp2 <- SelfControlledCaseSeries::createEraCovariateSettings(label = "Pre-exposure second dose",
                                                                      includeEraIds = "exposureId2",
                                                                      start = -30,
                                                                      end = -1,
                                                                      endAnchor = "era start")
  
    
  createSccsIntervalDataArgs1 <- SelfControlledCaseSeries::createCreateSccsIntervalDataArgs(eraCovariateSettings = list(covarExposureOfInt,
                                                                                                                        covarPreExp,
                                                                                                                        covarExposureOfInt2,
                                                                                                                        covarPreExp2))
  ageSettings <- SelfControlledCaseSeries::createAgeCovariateSettings(ageKnots = 5, 
                                                                      computeConfidenceIntervals = FALSE)
  
  seasonalitySettings <- SelfControlledCaseSeries::createSeasonalityCovariateSettings(seasonKnots = 5, 
                                                                                      computeConfidenceIntervals = FALSE)
  
  createSccsIntervalDataArgs2 <- SelfControlledCaseSeries::createCreateSccsIntervalDataArgs(eraCovariateSettings = list(covarExposureOfInt,
                                                                                                                        covarPreExp,
                                                                                                                        covarExposureOfInt2,
                                                                                                                        covarPreExp2),
                                                                                            ageCovariateSettings = ageSettings,
                                                                                            seasonalityCovariateSettings = seasonalitySettings)
  
  fitSccsModelArgs <- SelfControlledCaseSeries::createFitSccsModelArgs()
  
  sccsAnalysis1 <- SelfControlledCaseSeries::createSccsAnalysis(analysisId = 1,
                                                                description = "SCCS",
                                                                getDbSccsDataArgs = getDbSccsDataArgs,
                                                                createStudyPopulationArgs = createStudyPopulationArgs,
                                                                createSccsIntervalDataArgs = createSccsIntervalDataArgs1,
                                                                fitSccsModelArgs = fitSccsModelArgs)
  
  sccsAnalysis2 <- SelfControlledCaseSeries::createSccsAnalysis(analysisId = 2,
                                                                description = "SCCS adjusting for age and season",
                                                                getDbSccsDataArgs = getDbSccsDataArgs,
                                                                createStudyPopulationArgs = createStudyPopulationArgs,
                                                                createSccsIntervalDataArgs = createSccsIntervalDataArgs2,
                                                                fitSccsModelArgs = fitSccsModelArgs)
  
  sccsAnalysisList <- list(sccsAnalysis1, sccsAnalysis2)
  return(sccsAnalysisList)
}
