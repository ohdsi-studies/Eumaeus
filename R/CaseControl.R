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

runCaseControl <- function(connectionDetails,
                           cdmDatabaseSchema,
                           cohortDatabaseSchema,
                           cohortTable,
                           outputFolder,
                           maxCores) {
  start <- Sys.time()
  
  ccFolder <- file.path(outputFolder, "caseControl")
  if (!file.exists(ccFolder))
    dir.create(ccFolder)
  
  ccSummaryFile <- file.path(outputFolder, "ccSummary.csv")
  if (!file.exists(ccSummaryFile)) {
    allControls <- loadAllControls(outputFolder)
    
    exposureCohorts <- loadExposureCohorts(outputFolder) %>%
      filter(.data$sampled == FALSE & .data$comparator == FALSE)
    
    baseExposureIds <- exposureCohorts %>%
      distinct(.data$baseExposureId) %>%
      pull()
    allEstimates <- list()
    # baseExposureId <- baseExposureIds[1]
    for (baseExposureId in baseExposureIds) {
      exposures <- exposureCohorts %>%
        filter(.data$baseExposureId == !!baseExposureId) 
      
      controls <- allControls %>%
        filter(.data$exposureId == baseExposureId)
      
      exposureFolder <- file.path(ccFolder, sprintf("e_%s", baseExposureId))
      if (!file.exists(exposureFolder))
        dir.create(exposureFolder)
      
      timePeriods <- splitTimePeriod(startDate = controls$startDate[1], endDate = controls$endDate[1])
      timePeriods$folder <- file.path(exposureFolder, sprintf("ccOutput_t%d", timePeriods$seqId))
      sapply(timePeriods$folder[!file.exists(timePeriods$folder)], dir.create)
      
      # i <- nrow(timePeriods)
      for (i in nrow(timePeriods):1) {
        periodEstimatesFile <- file.path(exposureFolder, sprintf("estimates_t%d.csv", timePeriods$seqId[i]))
        if (!file.exists(periodEstimatesFile)) {
          ParallelLogger::logInfo(sprintf("Executing case-control for exposure %s and period: %s", baseExposureId, timePeriods$label[i]))
          periodFolder <- timePeriods$folder[i]
          
          estimates <- computeCcEstimates(connectionDetails = connectionDetails,
                                          cdmDatabaseSchema = cdmDatabaseSchema,
                                          cohortDatabaseSchema = cohortDatabaseSchema,
                                          cohortTable = cohortTable,
                                          exposures = exposures,
                                          outcomeIds = controls$outcomeId,
                                          periodFolder = periodFolder,
                                          startDate = controls$startDate[1],
                                          endDate = controls$endDate[1],
                                          maxCores = maxCores)
          if (i == nrow(timePeriods)) {
            # All prior periods don't need to download data, can subset data of last period:
            ParallelLogger::logInfo(sprintf("Subsetting CaseData for all periods for exposure %s", baseExposureId))
            cluster <- ParallelLogger::makeCluster(min(3, maxCores))
            ParallelLogger::clusterRequire(cluster, "Eumaeus")
            invisible(ParallelLogger::clusterApply(cluster = cluster, 
                                                   x = 1:(nrow(timePeriods) - 1), 
                                                   fun = subsetCaseData, 
                                                   timePeriods = timePeriods))
            ParallelLogger::stopCluster(cluster)
          }
          
          readr::write_csv(estimates, periodEstimatesFile)
        } else {
          estimates <- loadCmEstimates(periodEstimatesFile)
        }
        estimates$seqId <- timePeriods$seqId[i]
        allEstimates[[length(allEstimates) + 1]] <- estimates
      }
    }
    allEstimates <- bind_rows(allEstimates)  
    allEstimates <- allEstimates %>%
      filter(.data$exposedCases > 0) 
    readr::write_csv(allEstimates, ccSummaryFile)
  }
  delta <- Sys.time() - start
  message(paste("Completed case-control analyses in", signif(delta, 3), attr(delta, "units")))
}

getCaseData <- function(caseDataFile) {
  if (exists("caseData", envir = cache)) {
    caseData <- get("caseData", envir = cache)
  }
  if (!mget("caseDataFile", envir = cache, ifnotfound = "") == caseDataFile) {
    if (exists("caseData", envir = cache)) {
      Andromeda::close(caseData)
    }
    caseData <- CaseControl::loadCaseData(caseDataFile)
    assign("caseData", caseData, envir = cache)
    assign("caseDataFile", caseDataFile, envir = cache)
  }
  return(caseData)
}

subsetCaseData <- function(i, timePeriods) {
  caseDataFile <- file.path(timePeriods$folder[i], "caseData_cd1.zip")
  if (!file.exists(caseDataFile)) {
    bigCaseDataFile <- file.path(timePeriods$folder[nrow(timePeriods)], "caseData_cd1.zip")
    bigCaseData <- getCaseData(bigCaseDataFile)
    
    periodEndDate <-as.integer(timePeriods$endDate[i])
    # timePeriods$endDate[i] == Andromeda::restoreDate(periodEndDate)
    caseData <- Andromeda::andromeda()
    caseData$cases <- bigCaseData$cases %>%
      filter(.data$indexDate <= periodEndDate)
    caseData$exposures <- bigCaseData$exposures %>%
      filter(.data$exposureStartDate <= periodEndDate) %>%
      mutate(exposureEndDate = case_when(.data$exposureEndDate > periodEndDate ~ periodEndDate,
                                         TRUE ~ .data$exposureEndDate))
    caseData$nestingCohorts  <- bigCaseData$nestingCohorts  %>%
      filter(.data$startDate <= periodEndDate) %>%
      mutate(endDate = case_when(.data$endDate > periodEndDate ~ periodEndDate,
                                 TRUE ~ .data$endDate))
    Andromeda::createIndex(caseData$nestingCohorts, "nestingCohortId")
    Andromeda::createIndex(caseData$nestingCohorts, "personSeqId")
    metaData <- attr(bigCaseData, "metaData")
    attr(caseData, "metaData") <- metaData
    class(caseData) <- class(bigCaseData)
    CaseControl::saveCaseData(caseData, caseDataFile)
  }
}

computeCcEstimates <- function(connectionDetails,
                               cdmDatabaseSchema,
                               cohortDatabaseSchema,
                               cohortTable,
                               exposures,
                               outcomeIds,
                               startDate, 
                               endDate,
                               periodFolder,
                               maxCores) {
  ccAnalysisList <- createCcAnalysesList(startDate, endDate)
  exposureOutcomeList <- list()
  for (outcomeId in outcomeIds) {
    for (exposureId in exposures$exposureId) {
      exposureOutcome <- CaseControl::createExposureOutcomeNestingCohort(exposureId = exposureId,
                                                                         outcomeId = outcomeId)
      exposureOutcomeList[[length(exposureOutcomeList) + 1]] <- exposureOutcome
    }
  }
  
  referenceTable <- CaseControl::runCcAnalyses(connectionDetails = connectionDetails,
                                               cdmDatabaseSchema = cdmDatabaseSchema,
                                               exposureDatabaseSchema = cohortDatabaseSchema,
                                               exposureTable = cohortTable,
                                               outcomeDatabaseSchema = cohortDatabaseSchema,
                                               outcomeTable = cohortTable,
                                               tempEmulationSchema = cohortDatabaseSchema,
                                               outputFolder = periodFolder,
                                               ccAnalysisList = ccAnalysisList,
                                               exposureOutcomeNestingCohortList = exposureOutcomeList,
                                               prefetchExposureData = TRUE,
                                               getDbCaseDataThreads = min(3, maxCores),
                                               selectControlsThreads = min(5, maxCores),
                                               getDbExposureDataThreads = min(5, maxCores),
                                               createCaseControlDataThreads = min(5, maxCores),
                                               fitCaseControlModelThreads =  min(5, maxCores))
  
  estimates <- CaseControl::summarizeCcAnalyses(referenceTable, periodFolder)
  return(estimates)
}

createCcAnalysesList <- function(startDate, endDate) {
  
  prior <- Cyclops::createPrior("none")
  
  getDbCaseDataArgs1 <- CaseControl::createGetDbCaseDataArgs(useNestingCohort = FALSE, 
                                                             getVisits = FALSE,
                                                             studyStartDate = format(startDate, "%Y%m%d"),
                                                             studyEndDate = format(endDate, "%Y%m%d"),
                                                             maxCasesPerOutcome = 1e6)
  
  samplingCriteria <- CaseControl::createSamplingCriteria(controlsPerCase = 4,
                                                          seed = 123)
  
  selectControlsArgs1 <- CaseControl::createSelectControlsArgs(firstOutcomeOnly = TRUE,
                                                               washoutPeriod = 365,
                                                               controlSelectionCriteria = samplingCriteria)
  
  # Excluding one gender (8507 = male) explicitly to avoid redundancy:
  covariateSettings1 <- CaseControl::createSimpleCovariateSettings(useDemographicsAgeGroup = TRUE,
                                                                   useDemographicsGender = TRUE)
  
  getDbExposureDataArgs1 <- CaseControl::createGetDbExposureDataArgs(covariateSettings = covariateSettings1)
  
  createCaseControlDataArgs1_28 <- CaseControl::createCreateCaseControlDataArgs(firstExposureOnly = FALSE,
                                                                                riskWindowStart = -28,
                                                                                riskWindowEnd = -1, 
                                                                                exposureWashoutPeriod = 365)
  
  createCaseControlDataArgs1_42 <- CaseControl::createCreateCaseControlDataArgs(firstExposureOnly = FALSE,
                                                                                riskWindowStart = -42,
                                                                                riskWindowEnd = -1, 
                                                                                exposureWashoutPeriod = 365)
  
  createCaseControlDataArgs0_1 <- CaseControl::createCreateCaseControlDataArgs(firstExposureOnly = FALSE,
                                                                               riskWindowStart = -1,
                                                                               riskWindowEnd = 0, 
                                                                               exposureWashoutPeriod = 365)
  
  fitCaseControlModelArgs1 <- CaseControl::createFitCaseControlModelArgs(useCovariates = TRUE,
                                                                         prior = prior)
  
  ccAnalysis1 <- CaseControl::createCcAnalysis(analysisId = 1,
                                               description = "Sampling, adj. for age & sex, tar 1-28 days",
                                               getDbCaseDataArgs = getDbCaseDataArgs1,
                                               selectControlsArgs = selectControlsArgs1,
                                               getDbExposureDataArgs = getDbExposureDataArgs1,
                                               createCaseControlDataArgs = createCaseControlDataArgs1_28,
                                               fitCaseControlModelArgs = fitCaseControlModelArgs1)

  ccAnalysis3 <- CaseControl::createCcAnalysis(analysisId = 3,
                                               description = "Sampling, adj. for age & sex, tar 1-42 days",
                                               getDbCaseDataArgs = getDbCaseDataArgs1,
                                               selectControlsArgs = selectControlsArgs1,
                                               getDbExposureDataArgs = getDbExposureDataArgs1,
                                               createCaseControlDataArgs = createCaseControlDataArgs1_42,
                                               fitCaseControlModelArgs = fitCaseControlModelArgs1)
  
  ccAnalysis5 <- CaseControl::createCcAnalysis(analysisId = 5,
                                               description = "Sampling, adj. for age & sex, tar 0-1 days",
                                               getDbCaseDataArgs = getDbCaseDataArgs1,
                                               selectControlsArgs = selectControlsArgs1,
                                               getDbExposureDataArgs = getDbExposureDataArgs1,
                                               createCaseControlDataArgs = createCaseControlDataArgs0_1,
                                               fitCaseControlModelArgs = fitCaseControlModelArgs1)
  
  matchingCriteria <- CaseControl::createMatchingCriteria(controlsPerCase = 4,
                                                          matchOnAge = TRUE,
                                                          ageCaliper = 2,
                                                          matchOnGender = TRUE)
  
  selectControlsArgs2 <- CaseControl::createSelectControlsArgs(firstOutcomeOnly = FALSE,
                                                               washoutPeriod = 365,
                                                               controlSelectionCriteria = matchingCriteria)
  
  fitCaseControlModelArgs2 <- CaseControl::createFitCaseControlModelArgs(useCovariates = FALSE,
                                                                         prior = prior)
  
  ccAnalysis2 <- CaseControl::createCcAnalysis(analysisId = 2,
                                               description = "Matching on age & sex, tar 1-28 days",
                                               getDbCaseDataArgs = getDbCaseDataArgs1,
                                               selectControlsArgs = selectControlsArgs2,
                                               getDbExposureDataArgs = getDbExposureDataArgs1,
                                               createCaseControlDataArgs = createCaseControlDataArgs1_28,
                                               fitCaseControlModelArgs = fitCaseControlModelArgs2)

  ccAnalysis4 <- CaseControl::createCcAnalysis(analysisId = 4,
                                               description = "Matching on age & sex, tar 1-42 days",
                                               getDbCaseDataArgs = getDbCaseDataArgs1,
                                               selectControlsArgs = selectControlsArgs2,
                                               getDbExposureDataArgs = getDbExposureDataArgs1,
                                               createCaseControlDataArgs = createCaseControlDataArgs1_42,
                                               fitCaseControlModelArgs = fitCaseControlModelArgs2)

  ccAnalysis6 <- CaseControl::createCcAnalysis(analysisId = 6,
                                               description = "Matching on age & sex, tar 0-1 days",
                                               getDbCaseDataArgs = getDbCaseDataArgs1,
                                               selectControlsArgs = selectControlsArgs2,
                                               getDbExposureDataArgs = getDbExposureDataArgs1,
                                               createCaseControlDataArgs = createCaseControlDataArgs0_1,
                                               fitCaseControlModelArgs = fitCaseControlModelArgs2)
  
  ccAnalysisList <- list(ccAnalysis1, ccAnalysis2, ccAnalysis3, ccAnalysis4, ccAnalysis5, ccAnalysis6)
  return(ccAnalysisList)
}
