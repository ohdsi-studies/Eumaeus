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
    # baseExposureId <- baseExposureIds[3]
    for (baseExposureId in baseExposureIds) {
      exposures <- exposureCohorts %>%
        filter(.data$baseExposureId == !!baseExposureId) 
      
      controls <- allControls %>%
        filter(.data$exposureId == baseExposureId)
      
      exposureFolder <- file.path(ccFolder, sprintf("e_%s", baseExposureId))
      if (!file.exists(exposureFolder))
        dir.create(exposureFolder)
      
      timePeriods <- splitTimePeriod(startDate = controls$startDate[1], endDate = controls$endDate[1])
      periodFolders <- file.path(exposureFolder, sprintf("ccOutput_t%d", timePeriods$seqId))
      periodFolders <- periodFolders[!file.exists(periodFolders)]
      sapply(periodFolders, dir.create)
      # i <- nrow(timePeriods)
      for (i in nrow(timePeriods):1) {
        periodEstimatesFile <- file.path(exposureFolder, sprintf("estimates_t%d.csv", timePeriods$seqId[i]))
        if (!file.exists(periodEstimatesFile)) {
          ParallelLogger::logInfo(sprintf("Executing case-control for exposure %s and period: %s", baseExposureId, timePeriods$label[i]))
          periodFolder <- file.path(exposureFolder, sprintf("ccOutput_t%d", timePeriods$seqId[i]))
          
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
            ccDataFiles <- list.files(path = periodFolder, pattern = "CcData_l[0-9]+.zip")
            for (ccDataFile in ccDataFiles) {
              for (j in 1:(i-1)) {
                bigCcData <- NULL
                priorPeriodFolder <- file.path(exposureFolder, sprintf("ccOutput_t%d", timePeriods$seqId[j]))
                if (!file.exists(file.path(priorPeriodFolder, ccDataFile))) {
                  ParallelLogger::logInfo(sprintf("- Subsetting %s to %s", ccDataFile, timePeriods$label[j]))
                  if (is.null(bigCcData)) {
                    bigCcData <- SelfControlledCaseSeries::loadCcData(file.path(periodFolder, ccDataFile))
                  }
                  subsetCcData(bigCcData = bigCcData,
                               endDate = timePeriods$endDate[j],
                               ccDataFile = file.path(priorPeriodFolder, ccDataFile))
                  
                }
              }
            }
          }
          
          readr::write_csv(estimates, periodEstimatesFile)
        } else {
          estimates <- loadCmEstimates(periodEstimatesFile)
        }
        estimates$periodId <- timePeriods$seqId[i]
        allEstimates[[length(allEstimates) + 1]] <- estimates
      }
    }
    allEstimates <- bind_rows(allEstimates)  
    allEstimates <- allEstimates %>%
      filter(.data$exposedOutcomes > 0) %>%
      mutate(llr = llr(.data$exposedOutcomes, .data$expectedOutcomes))
    readr::write_csv(allEstimates, ccSummaryFile)
  }
  delta <- Sys.time() - start
  message(paste("Completed SCCS analyses in", signif(delta, 3), attr(delta, "units")))
}

subsetCcData <- function(bigCcData,
                         endDate,
                         ccDataFile) {
  ccData <- Andromeda::andromeda()
  
  ccData$cases <- bigCcData$cases %>%
    collect() %>%
    mutate(startDate = as.Date(paste(.data$startYear, .data$startMonth, .data$startDay, sep = "-"), format = "%Y-%m-%d")) %>%
    filter(.data$startDate < !!endDate) %>%
    mutate(newObservationDays = as.double(!!endDate - .data$startDate)) %>%
    mutate(noninformativeEndCensor = ifelse(.data$newObservationDays < .data$observationDays, 1L, .data$noninformativeEndCensor)) %>%
    mutate(observationDays = ifelse(.data$newObservationDays < .data$observationDays, .data$newObservationDays, .data$observationDays)) %>%
    select(-.data$startDate, -.data$newObservationDays)
  
  ccData$eraRef <- bigCcData$eraRef
  ccData$eras <- bigCcData$eras
  ccData$eras <- ccData$eras %>%
    inner_join(select(ccData$cases, .data$caseId, .data$observationDays), by = "caseId") %>%
    filter(.data$startDay < .data$observationDays) %>%
    select(-.data$observationDays)
  
  metaData <- attr(bigCcData, "metaData")
  attr(ccData, "metaData") <- metaData
  class(ccData) <- class(bigCcData)
  SelfControlledCaseSeries::saveCcData(ccData, ccDataFile)
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
  
  # exposureOutcomeList <- list(CaseControl::createExposureOutcomeNestingCohort(exposureId = 205021, outcomeId = 438120))
  
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
                                               getDbCaseDataThreads = 1,#min(3, maxCores),
                                               selectControlsThreads = min(5, maxCores),
                                               getDbExposureDataThreads = 1,#min(5, maxCores),
                                               createCaseControlDataThreads = 1,#min(5, maxCores),
                                               fitCaseControlModelThreads =  min(5, maxCores))
  
  estimates <- summarizeCcAnalyses(referenceTable, periodFolder)
  return(estimates)
}

summarizeCcAnalyses <- function(referenceTable, periodFolder) {
  # Custom summarize function because we want second dose as just another estimate
  result <- list()
  # i <- 576
  for (i in 1:nrow(referenceTable)) {
    # print(i)
    ccModel <- readRDS(file.path(periodFolder, as.character(referenceTable$ccModelFile[i])))
    attrition <- as.data.frame(ccModel$metaData$attrition)
    attrition <- attrition[nrow(attrition), ]
    row <- referenceTable[i, c("outcomeId", "analysisId")]
    row$outcomeSubjects <- attrition$outcomeSubjects
    row$outcomeEvents <- attrition$outcomeEvents
    row$outcomeObsPeriods <- attrition$outcomeObsPeriods
    estimates <- ccModel$estimates[grepl("^Main|^Second", ccModel$estimates$covariateName), ]
    if (!is.null(estimates) && nrow(estimates) != 0) {
      for (j in 1:nrow(estimates)) {
        row$exposureId <- estimates$originalEraId[j]
        row$rr <- exp(estimates$logRr[j])
        row$ci95lb <- exp(estimates$logLb95[j])
        row$ci95ub <- exp(estimates$logUb95[j])
        row$logRr <- estimates$logRr[j]
        row$seLogRr <- estimates$seLogRr[j]
        z <- row$logRr/row$seLogRr
        row$p <- 2 * pmin(pnorm(z), 1 - pnorm(z))
        covStats <- ccModel$metaData$covariateStatistics %>%
          filter(.data$covariateId == estimates$covariateId[j])
        row$exposedSubjects <- covStats$personCount
        row$exposedDays <- covStats$dayCount
        row$exposedOutcomes <- covStats$outcomeCount
        row$expectedOutcomes <- row$exposedOutcomes / row$rr
        result[[length(result) + 1]] <- row
      }
    } 
  }
  result <- bind_rows(result)
  return(result)
}

createCcAnalysesList <- function(startDate, endDate) {
  
  prior <- Cyclops::createPrior("none")
  
  getDbCaseDataArgs1 <- CaseControl::createGetDbCaseDataArgs(useNestingCohort = FALSE, 
                                                             getVisits = FALSE,
                                                             studyStartDate = format(startDate, "%Y%m%d"),
                                                             studyEndDate = format(endDate, "%Y%m%d"),
                                                             maxCasesPerOutcome = 1e6)
  
  samplingCriteria <- CaseControl::createSamplingCriteria(controlsPerCase = 2,
                                                          seed = 123)
  
  selectControlsArgs1 <- CaseControl::createSelectControlsArgs(firstOutcomeOnly = TRUE,
                                                               washoutPeriod = 365,
                                                               controlSelectionCriteria = samplingCriteria)
  
  # Excluding one gender (8507 = male) explicitly to avoid redundancy:
  covariateSettings1 <- CaseControl::createSimpleCovariateSettings(useDemographicsAgeGroup = TRUE,
                                                                   useDemographicsGender = TRUE)
  
  getDbExposureDataArgs1 <- CaseControl::createGetDbExposureDataArgs(covariateSettings = covariateSettings1)
  
  createCaseControlDataArgs1 <- CaseControl::createCreateCaseControlDataArgs(firstExposureOnly = FALSE,
                                                                             riskWindowStart = -28,
                                                                             riskWindowEnd = -1, 
                                                                             exposureWashoutPeriod = 365)
  
  fitCaseControlModelArgs1 <- CaseControl::createFitCaseControlModelArgs(useCovariates = TRUE,
                                                                         prior = prior)
  
  ccAnalysis1 <- CaseControl::createCcAnalysis(analysisId = 1,
                                               description = "Sampling, adj. for age & sex",
                                               getDbCaseDataArgs = getDbCaseDataArgs1,
                                               selectControlsArgs = selectControlsArgs1,
                                               getDbExposureDataArgs = getDbExposureDataArgs1,
                                               createCaseControlDataArgs = createCaseControlDataArgs1,
                                               fitCaseControlModelArgs = fitCaseControlModelArgs1)
  
  matchingCriteria <- CaseControl::createMatchingCriteria(controlsPerCase = 2,
                                                          matchOnAge = TRUE,
                                                          ageCaliper = 2,
                                                          matchOnGender = TRUE)
  
  selectControlsArgs2 <- CaseControl::createSelectControlsArgs(firstOutcomeOnly = FALSE,
                                                               washoutPeriod = 365,
                                                               controlSelectionCriteria = matchingCriteria)
  
  fitCaseControlModelArgs2 <- CaseControl::createFitCaseControlModelArgs(useCovariates = FALSE,
                                                                         prior = prior)
  
  ccAnalysis2 <- CaseControl::createCcAnalysis(analysisId = 2,
                                               description = "Matching on age & sex",
                                               getDbCaseDataArgs = getDbCaseDataArgs1,
                                               selectControlsArgs = selectControlsArgs2,
                                               getDbExposureDataArgs = getDbExposureDataArgs1,
                                               createCaseControlDataArgs = createCaseControlDataArgs1,
                                               fitCaseControlModelArgs = fitCaseControlModelArgs2)
  
  ccAnalysisList <- list(ccAnalysis1, ccAnalysis2)
  return(ccAnalysisList)
}
