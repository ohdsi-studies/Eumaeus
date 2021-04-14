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

runCohortMethod <- function(connectionDetails,
                            cdmDatabaseSchema,
                            cohortDatabaseSchema,
                            cohortTable,
                            outputFolder,
                            maxCores) {
  start <- Sys.time()
  
  cmFolder <- file.path(outputFolder, "cohortMethod")
  if (!file.exists(cmFolder))
    dir.create(cmFolder)
  
  cmSummaryFile <- file.path(outputFolder, "cmSummary.csv")
  if (!file.exists(cmSummaryFile)) {
    toExcludeFile <- file.path(outputFolder, "ToExclude.csv")
    if (!file.exists(toExcludeFile)) {
      ParallelLogger::logInfo("Comparing target to comparator cohorts to identify covariates to exclude")
      compareCohorts(connectionDetails = connectionDetails,
                     cdmDatabaseSchema = cdmDatabaseSchema,
                     cohortDatabaseSchema = cohortDatabaseSchema,
                     cohortTable = cohortTable,
                     outputFolder = outputFolder)
    }
    
    allControls <- loadAllControls(outputFolder)
    cohortsToCompare <- loadExposureCohorts(outputFolder) %>%
      filter(.data$sampled == TRUE)
    comparisons <- inner_join(filter(cohortsToCompare, .data$comparator == FALSE),
                              filter(cohortsToCompare, .data$comparator == TRUE),
                              by = c("baseExposureId", "shot"),
                              suffix = c("1", "2"))
    
    allEstimates <- list()
    # comparison <- comparisons[1, ]
    for (comparison in split(comparisons, 1:nrow(comparisons))) {
      targetId <- comparison$exposureId1
      comparatorId <- comparison$exposureId2
      crude <- grepl("crude", comparison$comparatorType2)
      controls <- allControls %>%
        filter(.data$exposureId == comparison$baseExposureId)
      targetComparatorFolder <- file.path(cmFolder, sprintf("t%s_c%s", targetId, comparatorId))
      if (!file.exists(targetComparatorFolder))
        dir.create(targetComparatorFolder)
      
      # Create one big CohortMethodData object ----------------------------
      bigCmDataFile <- file.path(targetComparatorFolder, "CmData.zip")
      if (!file.exists(bigCmDataFile)) {
        ParallelLogger::logInfo(sprintf("Constructing CohortMethodData object for target %s and comparator %s", targetId, comparatorId))
        if (crude) {
          covariateSettings <- FeatureExtraction::createCovariateSettings(useDemographicsGender = TRUE)
        } else {
          excludedCovariateConceptIds <- loadExposuresofInterest() %>%
            filter(.data$exposureId == !!comparison$baseExposureId) %>%
            pull(.data$conceptIdsToExclude) %>%
            strsplit(";")
          excludedCovariateConceptIds <- as.numeric(excludedCovariateConceptIds[[1]])
          additionalConceptToExclude <- loadAdditionalConceptsToExclude(outputFolder) %>%
            filter(.data$exposureId1 == targetId) %>%
            pull(.data$conceptId)
          excludedCovariateConceptIds <- unique(c(excludedCovariateConceptIds, additionalConceptToExclude))
          
          covariateSettings <- FeatureExtraction::createDefaultCovariateSettings(excludedCovariateConceptIds = excludedCovariateConceptIds,
                                                                                 addDescendantsToExclude = TRUE)
        }
        bigCmData <- CohortMethod::getDbCohortMethodData(connectionDetails = connectionDetails,
                                                         cdmDatabaseSchema = cdmDatabaseSchema,
                                                         exposureDatabaseSchema = cohortDatabaseSchema,
                                                         exposureTable = cohortTable,
                                                         outcomeDatabaseSchema = cohortDatabaseSchema,
                                                         outcomeTable = cohortTable,
                                                         targetId = targetId,
                                                         comparatorId = comparatorId,
                                                         outcomeIds = controls$outcomeId,
                                                         firstExposureOnly = FALSE,
                                                         washoutPeriod = 365,
                                                         covariateSettings = covariateSettings)
        
        CohortMethod::saveCohortMethodData(bigCmData, bigCmDataFile)
      } 
      bigCmData <- NULL
      timePeriods <- splitTimePeriod(startDate = controls$startDate[1], endDate = controls$endDate[1])
      timePeriods$folder <- file.path(targetComparatorFolder, sprintf("cmOutput_t%d", timePeriods$seqId))
      sapply(timePeriods$folder[!file.exists(timePeriods$folder)], dir.create)
      
      # Subset CohortMethodData for each period ------------------------------------------------
      ParallelLogger::logInfo(sprintf("Subsetting CohortMethodData for all periods for target %s and comparator %s", targetId, comparatorId))
      cluster <- ParallelLogger::makeCluster(min(3, maxCores))
      ParallelLogger::clusterRequire(cluster, "Eumaeus")
      invisible(ParallelLogger::clusterApply(cluster = cluster, 
                                             x = 1:nrow(timePeriods), 
                                             fun = subsetCmData, 
                                             targetId = targetId, 
                                             comparatorId = comparatorId,
                                             timePeriods = timePeriods,
                                             bigCmDataFile = bigCmDataFile) )
      ParallelLogger::stopCluster(cluster)
      
      # Fit shared propensity models per period ----------------------------------------------
      if (!crude) {
        ParallelLogger::logInfo(sprintf("Fitting propensity models across all periods for target %s and comparator %s", targetId, comparatorId))
        cluster <- ParallelLogger::makeCluster(min(max(1, floor(maxCores/5)), 5))
        invisible(ParallelLogger::clusterApply(cluster = cluster, 
                                               x = timePeriods$folder, 
                                               fun = fitSharedPsModel, 
                                               targetId = targetId, 
                                               comparatorId = comparatorId, 
                                               cvThreads = min(10, maxCores))) 
        ParallelLogger::stopCluster(cluster)
        
        # Output last propensity model:
        modelFile <- file.path(targetComparatorFolder, "PsModel.csv")
        if (!file.exists(modelFile)) {
          if (is.null(bigCmData)) {
            bigCmData <- CohortMethod::loadCohortMethodData(bigCmDataFile)
          }
          sharedPsFile <- file.path(timePeriods$folder[nrow(timePeriods)], sprintf("Ps_l1_p1_t%s_c%s.rds", targetId, comparatorId))
          ps <- readRDS(sharedPsFile)
          model <- CohortMethod::getPsModel(ps, bigCmData)
          readr::write_csv(model, modelFile)
        }
      }
      
      # Compute estimates --------------------------------------------------
      # i <- nrow(timePeriods)
      for (i in 1:nrow(timePeriods)) {
        periodEstimatesFile <- file.path(targetComparatorFolder, sprintf("estimates_t%d.csv", timePeriods$seqId[i]))
        if (!file.exists(periodEstimatesFile)) {
          ParallelLogger::logInfo(sprintf("Executing cohort method for target %s, comparator %s, and period: %s", targetId, comparatorId, timePeriods$label[i]))
          periodFolder <- file.path(targetComparatorFolder, sprintf("cmOutput_t%d", timePeriods$seqId[i]))
          estimates <- computeCohortMethodEstimates(targetId = targetId,
                                                    comparatorId = comparatorId,
                                                    outcomeIds = controls$outcomeId,
                                                    periodFolder = periodFolder,
                                                    maxCores = maxCores,
                                                    comparatorType = comparison$comparatorType2)
          readr::write_csv(estimates, periodEstimatesFile)
        } else {
          estimates <- loadCmEstimates(periodEstimatesFile)
        }
        estimates$seqId <- timePeriods$seqId[i]
        estimates$period <- timePeriods$label[i]
        allEstimates[[length(allEstimates) + 1]] <- estimates
      }
    }
    allEstimates <- bind_rows(allEstimates)  
    allEstimates <- allEstimates %>%
      filter(.data$eventsComparator > 0) %>%
      mutate(exposureId = .data$targetId,
             expectedOutcomes = .data$targetDays * (.data$eventsComparator / .data$comparatorDays)) 
    readr::write_csv(allEstimates, cmSummaryFile)
  }
  delta <- Sys.time() - start
  message(paste("Completed all per-month cohort method analyses in", signif(delta, 3), attr(delta, "units")))
}

getCohortMethodData <- function(cohortMethodDataFile) {
  if (exists("cohortMethodData", envir = cache)) {
    cohortMethodData <- get("cohortMethodData", envir = cache)
  }
  if (!mget("cohortMethodDataFile", envir = cache, ifnotfound = "") == cohortMethodDataFile) {
    if (exists("cohortMethodData", envir = cache)) {
      Andromeda::close(cohortMethodData)
    }
    cohortMethodData <- CohortMethod::loadCohortMethodData(cohortMethodDataFile)
    assign("cohortMethodData", cohortMethodData, envir = cache)
    assign("cohortMethodDataFile", cohortMethodDataFile, envir = cache)
  }
  return(cohortMethodData)
}

subsetCmData <- function(i,
                         targetId,
                         comparatorId,
                         timePeriods,
                         bigCmDataFile) {
  cmDataFile <- file.path(timePeriods$folder[i], sprintf("CmData_l1_t%s_c%s.zip", targetId, comparatorId))
  if (!file.exists(cmDataFile)) {
    bigCmData <- getCohortMethodData(bigCmDataFile)
    
    if (timePeriods$seqId[i] == max(timePeriods$seqId)) {
      cmData <- bigCmData
    } else {
      endDate <- as.integer(timePeriods$endDate[i])
      
      cmData <- Andromeda::andromeda()
      cmData$cohorts <- bigCmData$cohorts %>%
        filter(.data$cohortStartDate < endDate) %>%
        mutate(daysToObsEnd = ifelse(.data$daysToObsEnd > endDate - .data$cohortStartDate, 
                                     endDate - .data$cohortStartDate, 
                                     .data$daysToObsEnd))
      rowIds <- cmData$cohorts %>%
        pull(.data$rowId)
      cmData$outcomes <- bigCmData$outcomes %>%
        filter(.data$rowId %in% rowIds)
      cmData$covariates <- bigCmData$covariates %>%
        filter(.data$rowId %in% rowIds)
      
      cmData$analysisRef <- bigCmData$analysisRef
      cmData$covariateRef <- bigCmData$covariateRef
      
      metaData <- attr(bigCmData, "metaData")
      metaData$populationSize <- length(rowIds)
      attr(cmData, "metaData") <- metaData
      class(cmData) <- class(bigCmData)
    }
    CohortMethod::saveCohortMethodData(cmData, cmDataFile)
  }
  return(NULL)
}

fitSharedPsModel <- function(periodFolder, targetId, comparatorId, cvThreads) {
  sharedPsFile <- file.path(periodFolder, sprintf("Ps_l1_p1_t%s_c%s.rds", targetId, comparatorId))
  if (!file.exists(sharedPsFile)) {
    cmDataFile <- file.path(periodFolder, sprintf("CmData_l1_t%s_c%s.zip", targetId, comparatorId))
    cmData <- CohortMethod::loadCohortMethodData(cmDataFile)
    studyPop <- CohortMethod::createStudyPopulation(cohortMethodData = cmData,
                                                    removeSubjectsWithPriorOutcome = TRUE,
                                                    removeDuplicateSubjects = TRUE,
                                                    minDaysAtRisk = 1,
                                                    riskWindowStart = 0,
                                                    startAnchor = "cohort start",
                                                    riskWindowEnd = 28,
                                                    endAnchor = "cohort start")
    sharedPs <- CohortMethod::createPs(cohortMethodData = cmData,
                                       population = studyPop,
                                       errorOnHighCorrelation = TRUE,
                                       stopOnError = FALSE,
                                       maxCohortSizeForFitting = 150000,
                                       control = Cyclops::createControl(cvType = "auto",
                                                                        startingVariance = 0.01,
                                                                        noiseLevel = "quiet",
                                                                        tolerance  = 2e-07,
                                                                        fold = 10,
                                                                        cvRepetitions = 1,
                                                                        threads = cvThreads))
    saveRDS(sharedPs, sharedPsFile)
  }
  return(NULL)
}

computeCohortMethodEstimates <- function(targetId,
                                         comparatorId,
                                         outcomeIds,
                                         periodFolder,
                                         maxCores,
                                         comparatorType) {
  
  cmAnalysisList <- createCmAnalysisList(comparatorType = comparatorType)
  
  tcosList <- list()
  tcosList[[1]] <- CohortMethod::createTargetComparatorOutcomes(targetId = targetId,
                                                                comparatorId = comparatorId,
                                                                outcomeIds = outcomeIds)
  
  # Picking a single outcome of interest, so intermediate files will not be generated for all others:
  outcomeIdsOfInterest <- outcomeIds[1]
  
  cmResult <- CohortMethod::runCmAnalyses(connectionDetails = NULL,
                                          cdmDatabaseSchema = NULL,
                                          outputFolder = periodFolder,
                                          cmAnalysisList = cmAnalysisList,
                                          targetComparatorOutcomesList = tcosList,
                                          refitPsForEveryOutcome = FALSE,
                                          refitPsForEveryStudyPopulation = FALSE,
                                          getDbCohortMethodDataThreads = 1,
                                          createStudyPopThreads = min(3, maxCores),
                                          createPsThreads = 1,
                                          psCvThreads = min(10, maxCores),
                                          trimMatchStratifyThreads = min(10, maxCores),
                                          fitOutcomeModelThreads = min(max(1, floor(maxCores/2)), 8),
                                          outcomeCvThreads = min(2, maxCores),
                                          outcomeIdsOfInterest = outcomeIdsOfInterest)
  
  estimates <- CohortMethod::summarizeAnalyses(cmResult, periodFolder)
  return(estimates)
}

createCmAnalysisList <- function(comparatorType) {
  if (comparatorType == "Visit-anchored crude") {
    startAnalysisId <- 1
  } else if (comparatorType == "Visit-anchored age-sex stratified") {
    startAnalysisId <- 2  
  } else if (comparatorType == "Random day crude") {
    startAnalysisId <- 3
  } else if (comparatorType == "Random day age-sex stratified") {
    startAnalysisId <- 4  
  }
  startAnalysisIdCount <- 4
  
  # Not used. CohortMethodData object already created earlier for efficiency.
  getDbCmDataArgs <- CohortMethod::createGetDbCohortMethodDataArgs(covariateSettings = NULL)
  
  # warning: if you make changes here, also make them in the fitSharedPsModel function.
  createStudyPopArgs1_28 <- CohortMethod::createCreateStudyPopulationArgs(removeSubjectsWithPriorOutcome = TRUE,
                                                                          removeDuplicateSubjects = TRUE,
                                                                          minDaysAtRisk = 1,
                                                                          riskWindowStart = 1,
                                                                          startAnchor = "cohort start",
                                                                          riskWindowEnd = 28,
                                                                          endAnchor = "cohort start")
  
  createStudyPopArgs1_42 <- CohortMethod::createCreateStudyPopulationArgs(removeSubjectsWithPriorOutcome = TRUE,
                                                                          removeDuplicateSubjects = TRUE,
                                                                          minDaysAtRisk = 1,
                                                                          riskWindowStart = 1,
                                                                          startAnchor = "cohort start",
                                                                          riskWindowEnd = 42,
                                                                          endAnchor = "cohort start")
  
  createStudyPopArgs0_1 <- CohortMethod::createCreateStudyPopulationArgs(removeSubjectsWithPriorOutcome = TRUE,
                                                                         removeDuplicateSubjects = TRUE,
                                                                         minDaysAtRisk = 1,
                                                                         riskWindowStart = 0,
                                                                         startAnchor = "cohort start",
                                                                         riskWindowEnd = 1,
                                                                         endAnchor = "cohort start")
  
  fitOutcomeModelArgs1 <- CohortMethod::createFitOutcomeModelArgs(useCovariates = FALSE,
                                                                  modelType = "cox",
                                                                  stratified = FALSE)
  
  if (startAnalysisId %in% c(1,3)) {
    cmAnalysis1 <- CohortMethod::createCmAnalysis(analysisId = startAnalysisId,
                                                  description = "Crude cohort method, tar 1-28 days",
                                                  getDbCohortMethodDataArgs = getDbCmDataArgs,
                                                  createStudyPopArgs = createStudyPopArgs1_28,
                                                  fitOutcomeModel = TRUE,
                                                  fitOutcomeModelArgs = fitOutcomeModelArgs1)
    
    cmAnalysis2 <- CohortMethod::createCmAnalysis(analysisId = startAnalysisId + 1 * startAnalysisIdCount,
                                                  description = "Crude cohort method, tar 1-42 days",
                                                  getDbCohortMethodDataArgs = getDbCmDataArgs,
                                                  createStudyPopArgs = createStudyPopArgs1_42,
                                                  fitOutcomeModel = TRUE,
                                                  fitOutcomeModelArgs = fitOutcomeModelArgs1)
    
    cmAnalysis3 <- CohortMethod::createCmAnalysis(analysisId = startAnalysisId + 2 * startAnalysisIdCount,
                                                  description = "Crude cohort method, tar 0-1 days",
                                                  getDbCohortMethodDataArgs = getDbCmDataArgs,
                                                  createStudyPopArgs = createStudyPopArgs0_1,
                                                  fitOutcomeModel = TRUE,
                                                  fitOutcomeModelArgs = fitOutcomeModelArgs1)
    
    return(list(cmAnalysis1, cmAnalysis2, cmAnalysis3))
  } else {
    # Not used. Shared PS object already created earlier for efficiency.
    createPsArgs <- CohortMethod::createCreatePsArgs()
    
    matchOnPsArgs <- CohortMethod::createMatchOnPsArgs(maxRatio = 1)
    
    cmAnalysis1 <- CohortMethod::createCmAnalysis(analysisId = startAnalysisId,
                                                  description = "1-on-1 matching, tar 1-28 days",
                                                  getDbCohortMethodDataArgs = getDbCmDataArgs,
                                                  createStudyPopArgs = createStudyPopArgs1_28,
                                                  createPs = TRUE,
                                                  createPsArgs = createPsArgs,
                                                  matchOnPs = TRUE,
                                                  matchOnPsArgs = matchOnPsArgs,
                                                  fitOutcomeModel = TRUE,
                                                  fitOutcomeModelArgs = fitOutcomeModelArgs1)
    
    cmAnalysis2 <- CohortMethod::createCmAnalysis(analysisId = startAnalysisId + 1 * startAnalysisIdCount,
                                                  description = "1-on-1 matching, tar 1-42 days",
                                                  getDbCohortMethodDataArgs = getDbCmDataArgs,
                                                  createStudyPopArgs = createStudyPopArgs1_42,
                                                  createPs = TRUE,
                                                  createPsArgs = createPsArgs,
                                                  matchOnPs = TRUE,
                                                  matchOnPsArgs = matchOnPsArgs,
                                                  fitOutcomeModel = TRUE,
                                                  fitOutcomeModelArgs = fitOutcomeModelArgs1)
    
    cmAnalysis3 <- CohortMethod::createCmAnalysis(analysisId = startAnalysisId + 2 * startAnalysisIdCount,
                                                  description = "1-on-1 matching, tar 0-1 days",
                                                  getDbCohortMethodDataArgs = getDbCmDataArgs,
                                                  createStudyPopArgs = createStudyPopArgs0_1,
                                                  createPs = TRUE,
                                                  createPsArgs = createPsArgs,
                                                  matchOnPs = TRUE,
                                                  matchOnPsArgs = matchOnPsArgs,
                                                  fitOutcomeModel = TRUE,
                                                  fitOutcomeModelArgs = fitOutcomeModelArgs1)
    
    # Added later, hence the confusing numbering
    stratifyByPsArgs <- CohortMethod::createStratifyByPsArgs(numberOfStrata = 5, baseSelection = "target")
    
    fitOutcomeModelArgs2 <- CohortMethod::createFitOutcomeModelArgs(useCovariates = FALSE,
                                                                    modelType = "cox",
                                                                    stratified = TRUE)
    
    cmAnalysis4 <- CohortMethod::createCmAnalysis(analysisId = 12 + startAnalysisId / 2,
                                                  description = "stratification, tar 1-28 days",
                                                  getDbCohortMethodDataArgs = getDbCmDataArgs,
                                                  createStudyPopArgs = createStudyPopArgs1_28,
                                                  createPs = TRUE,
                                                  createPsArgs = createPsArgs,
                                                  stratifyByPs = TRUE,
                                                  stratifyByPsArgs = stratifyByPsArgs,
                                                  fitOutcomeModel = TRUE,
                                                  fitOutcomeModelArgs = fitOutcomeModelArgs2)
    
    cmAnalysis5 <- CohortMethod::createCmAnalysis(analysisId = 14 + startAnalysisId / 2,
                                                  description = "stratification, tar 1-42 days",
                                                  getDbCohortMethodDataArgs = getDbCmDataArgs,
                                                  createStudyPopArgs = createStudyPopArgs1_42,
                                                  createPs = TRUE,
                                                  createPsArgs = createPsArgs,
                                                  stratifyByPs = TRUE,
                                                  stratifyByPsArgs = stratifyByPsArgs,
                                                  fitOutcomeModel = TRUE,
                                                  fitOutcomeModelArgs = fitOutcomeModelArgs2)
    
    cmAnalysis6 <- CohortMethod::createCmAnalysis(analysisId = 16 + startAnalysisId / 2,
                                                  description = "stratification, tar 0-1 days",
                                                  getDbCohortMethodDataArgs = getDbCmDataArgs,
                                                  createStudyPopArgs = createStudyPopArgs0_1,
                                                  createPs = TRUE,
                                                  createPsArgs = createPsArgs,
                                                  stratifyByPs = TRUE,
                                                  stratifyByPsArgs = stratifyByPsArgs,
                                                  fitOutcomeModel = TRUE,
                                                  fitOutcomeModelArgs = fitOutcomeModelArgs2)
    
    trimyByIptwArgs <- CohortMethod::createTrimByIptwArgs(maxWeight = 10,
                                                          estimator = "att")
    
    fitOutcomeModelArgs3 <- CohortMethod::createFitOutcomeModelArgs(useCovariates = FALSE,
                                                                    modelType = "cox",
                                                                    stratified = FALSE,
                                                                    inversePtWeighting = TRUE,
                                                                    estimator = "att")
    
    cmAnalysis7 <- CohortMethod::createCmAnalysis(analysisId = 18 + startAnalysisId / 2,
                                                  description = "IPTW, tar 1-28 days",
                                                  getDbCohortMethodDataArgs = getDbCmDataArgs,
                                                  createStudyPopArgs = createStudyPopArgs1_28,
                                                  createPs = TRUE,
                                                  createPsArgs = createPsArgs,
                                                  trimByIptw = TRUE,
                                                  trimByIptwArgs = trimyByIptwArgs,
                                                  fitOutcomeModel = TRUE,
                                                  fitOutcomeModelArgs = fitOutcomeModelArgs3)
    
    cmAnalysis8 <- CohortMethod::createCmAnalysis(analysisId = 20 + startAnalysisId / 2,
                                                  description = "IPTW, tar 1-42 days",
                                                  getDbCohortMethodDataArgs = getDbCmDataArgs,
                                                  createStudyPopArgs = createStudyPopArgs1_42,
                                                  createPs = TRUE,
                                                  createPsArgs = createPsArgs,
                                                  trimByIptw = TRUE,
                                                  trimByIptwArgs = trimyByIptwArgs,
                                                  fitOutcomeModel = TRUE,
                                                  fitOutcomeModelArgs = fitOutcomeModelArgs3)
    
    cmAnalysis9 <- CohortMethod::createCmAnalysis(analysisId = 22 + startAnalysisId / 2,
                                                  description = "IPTW, tar 0-1 days",
                                                  getDbCohortMethodDataArgs = getDbCmDataArgs,
                                                  createStudyPopArgs = createStudyPopArgs0_1,
                                                  createPs = TRUE,
                                                  createPsArgs = createPsArgs,
                                                  trimByIptw = TRUE,
                                                  trimByIptwArgs = trimyByIptwArgs,
                                                  fitOutcomeModel = TRUE,
                                                  fitOutcomeModelArgs = fitOutcomeModelArgs3)
    
    return(list(cmAnalysis1, cmAnalysis2, cmAnalysis3, cmAnalysis4, cmAnalysis5, cmAnalysis6, cmAnalysis7, cmAnalysis8, cmAnalysis9))
  }
}
