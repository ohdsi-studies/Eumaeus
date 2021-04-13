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

runPerMonthCohortMethod <- function(connectionDetails,
                                    cdmDatabaseSchema,
                                    cohortDatabaseSchema,
                                    cohortTable,
                                    outputFolder,
                                    maxCores) {
  start <- Sys.time()
  
  exposureCohorts <- loadExposureCohorts(outputFolder) %>%
    filter(.data$baseExposureId == 21184) # H1N1 vaccines
  if (nrow(exposureCohorts) == 0) {
    return(NULL)
  }
  
  cmFolder <- file.path(outputFolder, "cohortMethod")
  cmSummaryFile <- file.path(outputFolder, "cmSummary.csv")
  if (!file.exists(cmSummaryFile)) {
    stop("The cohort method (using cumulative data) must be executed first")
  }
  cmSummary <- loadEstimates(cmSummaryFile)
  if (all(cmSummary$analysisId < 25)) {
    allControls <- loadAllControls(outputFolder)
    cohortsToCompare <- exposureCohorts %>%
      filter(.data$sampled == TRUE & !grepl("crude", .data$comparatorType))
    comparisons <- inner_join(filter(cohortsToCompare, .data$comparator == FALSE),
                              filter(cohortsToCompare, .data$comparator == TRUE),
                              by = c("baseExposureId", "shot"),
                              suffix = c("1", "2"))
    
    allEstimates <- list()
    # comparison <- comparisons[1, ]
    for (comparison in split(comparisons, 1:nrow(comparisons))) {
      targetId <- comparison$exposureId1
      comparatorId <- comparison$exposureId2
      controls <- allControls %>%
        filter(.data$exposureId == comparison$baseExposureId)
      targetComparatorFolder <- file.path(cmFolder, sprintf("t%s_c%s", targetId, comparatorId))
      timePeriods <- splitTimePeriod(startDate = controls$startDate[1], endDate = controls$endDate[1])
      timePeriods$folder <- file.path(targetComparatorFolder, sprintf("cmOutput_t%d", timePeriods$seqId))
      
      # Fit shared propensity models per period ----------------------------------------------
      ParallelLogger::logInfo(sprintf("Fitting propensity models per period for target %s and comparator %s", targetId, comparatorId))
      cluster <- ParallelLogger::makeCluster(min(max(1, floor(maxCores/5)), 5))
      ParallelLogger::clusterRequire(cluster, "dplyr") # Remove this
      invisible(ParallelLogger::clusterApply(cluster = cluster, 
                                             x = 1:nrow(timePeriods), 
                                             fun = fitPerMonthSharedPsModel, 
                                             timePeriods = timePeriods,
                                             targetId = targetId, 
                                             comparatorId = comparatorId, 
                                             cvThreads = min(10, maxCores))) 
      ParallelLogger::stopCluster(cluster)
      
      # Create matched populations and fit outcome models -----------------------------------------------------
      for (i in 1:nrow(timePeriods)) {
        periodEstimatesFile <- file.path(targetComparatorFolder, sprintf("perMonthPsestimates_t%d.csv", timePeriods$seqId[i]))
        if (!file.exists(periodEstimatesFile)) {
          ParallelLogger::logInfo(sprintf("Executing per-month PS cohort method for target %s, comparator %s, and period: %s", targetId, comparatorId, timePeriods$label[i]))
          cluster <- ParallelLogger::makeCluster(min(max(1, floor(maxCores/2)), 8))
          ParallelLogger::clusterRequire(cluster, "dplyr") # Remove this
          omr <- ParallelLogger::clusterApply(cluster = cluster, 
                                              x = controls$outcomeId, 
                                              fun = matchPopAndfitModelPerMonth, 
                                              timePeriodIdx = i,
                                              timePeriods = timePeriods,
                                              targetId = targetId, 
                                              comparatorId = comparatorId, 
                                              threads = min(2, maxCores),
                                              comparatorType = comparison$comparatorType2)
          ParallelLogger::stopCluster(cluster)
          omr <- bind_rows(omr)
          saveRDS(omr, file.path(timePeriods$folder[i], "perMonthOutcomeModelReference.rds"))
          estimates <- CohortMethod::summarizeAnalyses(omr, file.path(timePeriods$folder[i]))
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
    cmSummary <- bind_rows(cmSummary, allEstimates)
    readr::write_csv(cmSummary, cmSummaryFile)
  }
}

matchPopAndfitModelPerMonth <- function(outcomeId, timePeriodIdx, timePeriods, targetId, comparatorId, threads, comparatorType) {
  tars <- tibble(start = c(1, 1, 0),
                 end = c(28, 42, 1))
  if (comparatorType == "Visit-anchored age-sex stratified") {
    tars$analysisId <- c(25, 27, 29)
  } else {
    tars$analysisId <- c(26, 28, 30)
  }
  tars$outcomeModelFile <- file.path(sprintf("Analysis_%s", tars$analysisId),
                                     sprintf("om_t%s_c%s_o%s.rds", targetId, comparatorId, outcomeId))
  
  sharedPs <- NULL
  analysisFolders <- file.path(timePeriods$folder[timePeriodIdx], sprintf("Analysis_%d", tars$analysisId))
  sapply(analysisFolders[!file.exists(analysisFolders)], dir.create)
  for (t in 1:nrow(tars)) {
    matchedPopFile <- file.path(timePeriods$folder[timePeriodIdx], 
                                sprintf("PerMonthMatchedPop_l1_p%d_t%s_c%s_o%s.rds", t, targetId, comparatorId, outcomeId))
    if (file.exists(matchedPopFile)) {
      matchedPop <- readRDS(matchedPopFile)
    } else {
      cmDataFile <- file.path(timePeriods$folder[timePeriodIdx], sprintf("CmData_l1_t%s_c%s.zip", targetId, comparatorId))
      cmData <- Eumaeus:::getCohortMethodData(cmDataFile)
      
      if (is.null(sharedPs)) {
        sharedPsFile <- file.path(timePeriods$folder[timePeriodIdx], sprintf("PerMonthPs_l1_p1_t%s_c%s.rds", targetId, comparatorId))
        sharedPs <- readRDS(sharedPsFile)
      }
      studyPop <- CohortMethod::createStudyPopulation(cohortMethodData = cmData,
                                                      outcomeId = outcomeId,
                                                      removeSubjectsWithPriorOutcome = TRUE,
                                                      removeDuplicateSubjects = TRUE,
                                                      minDaysAtRisk = 1,
                                                      riskWindowStart = tars$start[t],
                                                      startAnchor = "cohort start",
                                                      riskWindowEnd = tars$end[t],
                                                      endAnchor = "cohort start")
      ps <- studyPop %>%
        inner_join(select(sharedPs, .data$rowId, .data$propensityScore, .data$preferenceScore), by = "rowId")
      matchedPop <- CohortMethod::matchOnPs(ps, maxRatio = 1)
      if (timePeriodIdx > 1) {
        oldMatchedPopFile <- file.path(timePeriods$folder[timePeriodIdx - 1], 
                                       sprintf("PerMonthMatchedPop_l1_p%d_t%s_c%s_o%s.rds", t, targetId, comparatorId, outcomeId))
        oldMatchedPop <- readRDS(oldMatchedPopFile) %>%
          select(.data$rowId, .data$propensityScore, .data$preferenceScore, .data$stratumId) %>%
          inner_join(studyPop, by = "rowId")
        oldMatchedPop <- oldMatchedPop[, colnames(matchedPop)]
        
        matchedPop <- matchedPop %>%
          mutate(stratumId = .data$stratumId + 1 + max(oldMatchedPop$stratumId))
        
        matchedPop <- bind_rows(oldMatchedPop, matchedPop)
      }
      saveRDS(matchedPop, matchedPopFile)
    }
    
    outcomeModelFile <- file.path(timePeriods$folder[timePeriodIdx], tars$outcomeModelFile[t])
    if (!file.exists(outcomeModelFile)) {
      control <- Cyclops::createControl(cvType = "auto", 
                                        seed = 1, 
                                        startingVariance = 0.01, 
                                        tolerance = 2e-07, 
                                        cvRepetitions = 10, 
                                        threads = threads,
                                        noiseLevel = "quiet")
      outcomeModel <- CohortMethod::fitOutcomeModel(population = matchedPop,
                                                    modelType = "cox",
                                                    stratified = FALSE,
                                                    useCovariates = FALSE,
                                                    control = control)
      saveRDS(outcomeModel, outcomeModelFile)
    }
  }
  omr <- tars %>%
    select(.data$analysisId, .data$outcomeModelFile) %>%
    mutate(targetId = !!targetId,
           comparatorId = !!comparatorId,
           outcomeId = !!outcomeId)
  return(omr)
}

# i = 2
fitPerMonthSharedPsModel <- function(i, timePeriods, targetId, comparatorId, cvThreads) {
  sharedPsFile <- file.path(timePeriods$folder[i], sprintf("PerMonthPs_l1_p1_t%s_c%s.rds", targetId, comparatorId))
  if (!file.exists(sharedPsFile)) {
    cmDataFile <- file.path(timePeriods$folder[i], sprintf("CmData_l1_t%s_c%s.zip", targetId, comparatorId))
    cmData <- CohortMethod::loadCohortMethodData(cmDataFile)
    
    if (i > 1) {
      # Remove data of previous periods:
      startDate <- as.integer(timePeriods$endDate[i - 1] + 1)
      cmData$cohorts <- cmData$cohorts %>%
        filter(.data$cohortStartDate >= startDate)
      rowIds <- cmData$cohorts %>%
        pull(.data$rowId)
      cmData$outcomes <- cmData$outcomes %>%
        filter(.data$rowId %in% rowIds)
      cmData$covariates <- cmData$covariates %>%
        filter(.data$rowId %in% rowIds)
    }
    
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
