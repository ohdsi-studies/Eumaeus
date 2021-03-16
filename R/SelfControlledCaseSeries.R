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
    # baseExposureId <- baseExposureIds[1]
    for (baseExposureId in baseExposureIds) {
      exposures <- exposureCohorts %>%
        filter(.data$baseExposureId == !!baseExposureId) 
      
      controls <- allControls %>%
        filter(.data$exposureId == baseExposureId)
      
      exposureFolder <- file.path(sccsFolder, sprintf("e_%s", baseExposureId))
      if (!file.exists(exposureFolder))
        dir.create(exposureFolder)
      
      timePeriods <- splitTimePeriod(startDate = controls$startDate[1], endDate = controls$endDate[1])
      timePeriods$folder <- file.path(exposureFolder, sprintf("sccsOutput_t%d", timePeriods$seqId))
      sapply(timePeriods$folder[!file.exists(timePeriods$folder)], dir.create)
      # i <- nrow(timePeriods)
      for (i in nrow(timePeriods):1) {
        periodEstimatesFile <- file.path(exposureFolder, sprintf("estimates_t%d.csv", timePeriods$seqId[i]))
        if (!file.exists(periodEstimatesFile)) {
          ParallelLogger::logInfo(sprintf("Executing SCCS for exposure %s and period: %s", baseExposureId, timePeriods$label[i]))
          periodFolder <- timePeriods$folder[i]
          estimates <- computeSccsEstimates(connectionDetails = connectionDetails,
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
            ParallelLogger::logInfo(sprintf("Subsetting SccsData objects for all periods for exposure %s", baseExposureId))
            sccsDataFiles <- list.files(path = periodFolder, pattern = "SccsData_l[0-9]+.zip")
            cluster <- ParallelLogger::makeCluster(min(4, maxCores))
            ParallelLogger::clusterRequire(cluster, "Eumaeus")
            invisible(ParallelLogger::clusterApply(cluster = cluster, 
                                                   x = sccsDataFiles, 
                                                   fun = Eumaeus:::subsetSccsData, 
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
      filter(.data$exposedOutcomes > 0)
    readr::write_csv(allEstimates, sccsSummaryFile)
  }
  delta <- Sys.time() - start
  message(paste("Completed SCCS analyses in", signif(delta, 3), attr(delta, "units")))
}

subsetSccsData <- function(sccsDataFile, timePeriods) {
  for (i in 1:(nrow(timePeriods)-1)) {
    bigSccsData <- NULL
    priorPeriodFolder <- timePeriods$folder[i]
    if (!file.exists(file.path(priorPeriodFolder, sccsDataFile))) {
      ParallelLogger::logInfo(sprintf("- Subsetting %s to %s", sccsDataFile, timePeriods$label[i]))
      if (is.null(bigSccsData)) {
        lastPeriodFolder <- timePeriods$folder[nrow(timePeriods)]
        bigSccsData <- SelfControlledCaseSeries::loadSccsData(file.path(lastPeriodFolder, sccsDataFile))
      }
      subsetSingleSccsDataObject(bigSccsData = bigSccsData,
                                 endDate = timePeriods$endDate[i],
                                 sccsDataFile = file.path(priorPeriodFolder, sccsDataFile))
    }
  }
}

subsetSingleSccsDataObject <- function(bigSccsData,
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

computeSccsEstimates <- function(connectionDetails,
                                 cdmDatabaseSchema,
                                 cohortDatabaseSchema,
                                 cohortTable,
                                 exposures,
                                 outcomeIds,
                                 startDate, 
                                 endDate,
                                 periodFolder,
                                 maxCores) {
  sccsAnalysisList <- createSccsAnalysesList(startDate, endDate)
  exposureOutcomeList <- list()
  for (outcomeId in outcomeIds) {
    if (nrow(exposures) == 1) {
      exposureOutcome <- SelfControlledCaseSeries::createExposureOutcome(exposureId = exposures$exposureId,
                                                                         exposureId2 = -1,
                                                                         outcomeId = outcomeId)
      exposureOutcomeList[[length(exposureOutcomeList) + 1]] <- exposureOutcome
    } else {
      exposureOutcome <- SelfControlledCaseSeries::createExposureOutcome(exposureId = exposures$exposureId[exposures$shot == "Both"],
                                                                         exposureId2 = -1,
                                                                         outcomeId = outcomeId)
      exposureOutcomeList[[length(exposureOutcomeList) + 1]] <- exposureOutcome
      
      exposureOutcome <- SelfControlledCaseSeries::createExposureOutcome(exposureId = exposures$exposureId[exposures$shot == "First"],
                                                                         exposureId2 = exposures$exposureId[exposures$shot == "Second"],
                                                                         outcomeId = outcomeId)
      exposureOutcomeList[[length(exposureOutcomeList) + 1]] <- exposureOutcome
    }
  }
  
  referenceTable <- SelfControlledCaseSeries::runSccsAnalyses(connectionDetails = connectionDetails,
                                                              cdmDatabaseSchema = cdmDatabaseSchema,
                                                              outcomeDatabaseSchema = cohortDatabaseSchema,
                                                              outcomeTable = cohortTable,
                                                              exposureDatabaseSchema = cohortDatabaseSchema,
                                                              exposureTable = cohortTable,
                                                              outputFolder = periodFolder,
                                                              sccsAnalysisList = sccsAnalysisList,
                                                              exposureOutcomeList = exposureOutcomeList,
                                                              combineDataFetchAcrossOutcomes = FALSE,
                                                              getDbSccsDataThreads = min(4, maxCores),
                                                              createStudyPopulationThreads = min(10, maxCores),
                                                              createSccsIntervalDataThreads = min(10, maxCores),
                                                              fitSccsModelThreads = min(10, floor(maxCores/4)),
                                                              cvThreads = 4)
  
  estimates <- summarizeSccsAnalyses(referenceTable, periodFolder)
  return(estimates)
}

summarizeSccsAnalyses <- function(referenceTable, periodFolder) {
  # Custom summarize function because we want second dose as just another estimate
  result <- list()
  # i <- 10
  for (i in 1:nrow(referenceTable)) {
    # print(i)
    sccsModel <- readRDS(file.path(periodFolder, as.character(referenceTable$sccsModelFile[i])))
    attrition <- as.data.frame(sccsModel$metaData$attrition)
    attrition <- attrition[nrow(attrition), ]
    row <- referenceTable[i, c("outcomeId", "analysisId")]
    row$outcomeSubjects <- attrition$outcomeSubjects
    row$outcomeEvents <- attrition$outcomeEvents
    row$outcomeObsPeriods <- attrition$outcomeObsPeriods
    estimates <- sccsModel$estimates[grepl("^Main|^Second", sccsModel$estimates$covariateName), ]
    if (!is.null(estimates) && nrow(estimates) != 0) {
      for (j in 1:nrow(estimates)) {
        row$exposureId <- estimates$originalEraId[j]
        row$rr <- exp(estimates$logRr[j])
        row$ci95lb <- exp(estimates$logLb95[j])
        row$ci95ub <- exp(estimates$logUb95[j])
        row$logRr <- estimates$logRr[j]
        row$seLogRr <- estimates$seLogRr[j]
        row$llr <- estimates$llr[j]
        z <- row$logRr/row$seLogRr
        row$p <- 2 * pmin(pnorm(z), 1 - pnorm(z))
        covStats <- sccsModel$metaData$covariateStatistics %>%
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

createSccsAnalysesList <- function(startDate, endDate) {
  
  
  getDbSccsDataArgs <- SelfControlledCaseSeries::createGetDbSccsDataArgs(studyStartDate = format(startDate, "%Y%m%d"),
                                                                         studyEndDate = format(endDate, "%Y%m%d"),
                                                                         maxCasesPerOutcome = 250000,
                                                                         deleteCovariatesSmallCount = 5,
                                                                         exposureIds = c("exposureId", "exposureId2"))
  
  createStudyPopulationArgs <- SelfControlledCaseSeries::createCreateStudyPopulationArgs(naivePeriod = 365,
                                                                                         firstOutcomeOnly = FALSE)
  
  covarExposureOfInt1_28 <- SelfControlledCaseSeries::createEraCovariateSettings(label = "Main",
                                                                                 includeEraIds = "exposureId",
                                                                                 start = 1,
                                                                                 startAnchor = "era start",
                                                                                 end = 28,
                                                                                 endAnchor = "era start")
  
  covarExposureOfInt1_42 <- SelfControlledCaseSeries::createEraCovariateSettings(label = "Main",
                                                                                 includeEraIds = "exposureId",
                                                                                 start = 1,
                                                                                 startAnchor = "era start",
                                                                                 end = 42,
                                                                                 endAnchor = "era start")
  
  covarExposureOfInt0_1 <- SelfControlledCaseSeries::createEraCovariateSettings(label = "Main",
                                                                                includeEraIds = "exposureId",
                                                                                start = 0,
                                                                                startAnchor = "era start",
                                                                                end = 1,
                                                                                endAnchor = "era start")
  
  covarPreExp <- SelfControlledCaseSeries::createEraCovariateSettings(label = "Pre-exposure",
                                                                      includeEraIds = "exposureId",
                                                                      start = -30,
                                                                      end = -1,
                                                                      endAnchor = "era start")
  
  covarExposureOfInt2nd1_28 <- SelfControlledCaseSeries::createEraCovariateSettings(label = "Second",
                                                                                    includeEraIds = "exposureId2",
                                                                                    start = 1,
                                                                                    startAnchor = "era start",
                                                                                    end = 28,
                                                                                    endAnchor = "era start")
  
  covarExposureOfInt2nd1_42 <- SelfControlledCaseSeries::createEraCovariateSettings(label = "Second",
                                                                                    includeEraIds = "exposureId2",
                                                                                    start = 1,
                                                                                    startAnchor = "era start",
                                                                                    end = 42,
                                                                                    endAnchor = "era start")
  
  covarExposureOfInt2nd0_1 <- SelfControlledCaseSeries::createEraCovariateSettings(label = "Second",
                                                                                   includeEraIds = "exposureId2",
                                                                                   start = 0,
                                                                                   startAnchor = "era start",
                                                                                   end = 1,
                                                                                   endAnchor = "era start")
  
  covarPreExp2nd <- SelfControlledCaseSeries::createEraCovariateSettings(label = "Pre-exposure second",
                                                                       includeEraIds = "exposureId2",
                                                                       start = -30,
                                                                       end = -1,
                                                                       endAnchor = "era start")
  
  createSccsIntervalDataArgsSimple1_28 <- SelfControlledCaseSeries::createCreateSccsIntervalDataArgs(
    eraCovariateSettings = list(covarExposureOfInt1_28,
                                covarPreExp,
                                covarExposureOfInt2nd1_28,
                                covarPreExp2nd)
  )
  
  createSccsIntervalDataArgsSimple1_42 <- SelfControlledCaseSeries::createCreateSccsIntervalDataArgs(
    eraCovariateSettings = list(covarExposureOfInt1_42,
                                covarPreExp,
                                covarExposureOfInt2nd1_42,
                                covarPreExp2nd)
  )

  createSccsIntervalDataArgsSimple0_1 <- SelfControlledCaseSeries::createCreateSccsIntervalDataArgs(
    eraCovariateSettings = list(covarExposureOfInt0_1,
                                covarPreExp,
                                covarExposureOfInt2nd0_1,
                                covarPreExp2nd)
  )
  
  ageSettings <- SelfControlledCaseSeries::createAgeCovariateSettings(ageKnots = 5, 
                                                                      computeConfidenceIntervals = FALSE)
  
  seasonalitySettings <- SelfControlledCaseSeries::createSeasonalityCovariateSettings(seasonKnots = 5, 
                                                                                      computeConfidenceIntervals = FALSE)

  createSccsIntervalDataArgsAgeSeason1_28 <- SelfControlledCaseSeries::createCreateSccsIntervalDataArgs(
    eraCovariateSettings = list(covarExposureOfInt1_28,
                                covarPreExp,
                                covarExposureOfInt2nd1_28,
                                covarPreExp2nd),
    ageCovariateSettings = ageSettings,
    seasonalityCovariateSettings = seasonalitySettings
  )

  createSccsIntervalDataArgsAgeSeason1_42 <- SelfControlledCaseSeries::createCreateSccsIntervalDataArgs(
    eraCovariateSettings = list(covarExposureOfInt1_42,
                                covarPreExp,
                                covarExposureOfInt2nd1_42,
                                covarPreExp2nd),
    ageCovariateSettings = ageSettings,
    seasonalityCovariateSettings = seasonalitySettings
  )
  
  createSccsIntervalDataArgsAgeSeason0_1 <- SelfControlledCaseSeries::createCreateSccsIntervalDataArgs(
    eraCovariateSettings = list(covarExposureOfInt0_1,
                                covarPreExp,
                                covarExposureOfInt2nd0_1,
                                covarPreExp2nd),
    ageCovariateSettings = ageSettings,
    seasonalityCovariateSettings = seasonalitySettings
  )
  

  controlIntervalSettings1 <- SelfControlledCaseSeries::createControlIntervalSettings(
    includeEraIds = c("exposureId", "exposureId2"),
    start = -43,
    startAnchor = "era start",
    end = -15,
    endAnchor = "era start"
  )
  
  createScriIntervalDataArgs1_28 <- SelfControlledCaseSeries::createCreateScriIntervalDataArgs(
    eraCovariateSettings = list(covarExposureOfInt1_28,
                                covarExposureOfInt2nd1_28),
    controlIntervalSettings = controlIntervalSettings1
  )

  createScriIntervalDataArgs1_42 <- SelfControlledCaseSeries::createCreateScriIntervalDataArgs(
    eraCovariateSettings = list(covarExposureOfInt1_42,
                                covarExposureOfInt2nd1_42),
    controlIntervalSettings = controlIntervalSettings1
  )

  createScriIntervalDataArgs0_1 <- SelfControlledCaseSeries::createCreateScriIntervalDataArgs(
    eraCovariateSettings = list(covarExposureOfInt0_1,
                                covarExposureOfInt2nd0_1),
    controlIntervalSettings = controlIntervalSettings1
  )
  
  controlIntervalSettings2 <- SelfControlledCaseSeries::createControlIntervalSettings(
    includeEraIds = c("exposureId", "exposureId2"),
    start = 43,
    startAnchor = "era start",
    end = 71,
    endAnchor = "era start"
  )
  
  createScriIntervalDataArgsPost1_28 <- SelfControlledCaseSeries::createCreateScriIntervalDataArgs(
    eraCovariateSettings = list(covarExposureOfInt1_28,
                                covarExposureOfInt2nd1_28),
    controlIntervalSettings = controlIntervalSettings2
  )
  
  createScriIntervalDataArgsPost1_42 <- SelfControlledCaseSeries::createCreateScriIntervalDataArgs(
    eraCovariateSettings = list(covarExposureOfInt1_42,
                                covarExposureOfInt2nd1_42),
    controlIntervalSettings = controlIntervalSettings2
  )
  
  createScriIntervalDataArgsPost0_1 <- SelfControlledCaseSeries::createCreateScriIntervalDataArgs(
    eraCovariateSettings = list(covarExposureOfInt0_1,
                                covarExposureOfInt2nd0_1),
    controlIntervalSettings = controlIntervalSettings2
  )
  
  fitSccsModelArgs <- SelfControlledCaseSeries::createFitSccsModelArgs()
  
  sccsAnalysis1 <- SelfControlledCaseSeries::createSccsAnalysis(analysisId = 1,
                                                                description = "SCCS, tar 1-28 days",
                                                                getDbSccsDataArgs = getDbSccsDataArgs,
                                                                createStudyPopulationArgs = createStudyPopulationArgs,
                                                                createSccsIntervalDataArgs = createSccsIntervalDataArgsSimple1_28,
                                                                fitSccsModelArgs = fitSccsModelArgs)
  
  sccsAnalysis2 <- SelfControlledCaseSeries::createSccsAnalysis(analysisId = 2,
                                                                description = "SCCS adjusting for age and season, tar 1-28 days",
                                                                getDbSccsDataArgs = getDbSccsDataArgs,
                                                                createStudyPopulationArgs = createStudyPopulationArgs,
                                                                createSccsIntervalDataArgs = createSccsIntervalDataArgsAgeSeason1_28,
                                                                fitSccsModelArgs = fitSccsModelArgs)
  
  sccsAnalysis3 <- SelfControlledCaseSeries::createSccsAnalysis(analysisId = 3,
                                                                description = "SCRI with prior control interval, tar 1-28 days",
                                                                getDbSccsDataArgs = getDbSccsDataArgs,
                                                                createStudyPopulationArgs = createStudyPopulationArgs,
                                                                design = "SCRI",
                                                                createScriIntervalDataArgs = createScriIntervalDataArgs1_28,
                                                                fitSccsModelArgs = fitSccsModelArgs)
  
  sccsAnalysis4 <- SelfControlledCaseSeries::createSccsAnalysis(analysisId = 4,
                                                                description = "SCRI with posterior control interval, tar 1-28 days",
                                                                getDbSccsDataArgs = getDbSccsDataArgs,
                                                                createStudyPopulationArgs = createStudyPopulationArgs,
                                                                design = "SCRI",
                                                                createScriIntervalDataArgs = createScriIntervalDataArgsPost1_28,
                                                                fitSccsModelArgs = fitSccsModelArgs)

  
  sccsAnalysis5 <- SelfControlledCaseSeries::createSccsAnalysis(analysisId = 5,
                                                                description = "SCCS, tar 1-42 days",
                                                                getDbSccsDataArgs = getDbSccsDataArgs,
                                                                createStudyPopulationArgs = createStudyPopulationArgs,
                                                                createSccsIntervalDataArgs = createSccsIntervalDataArgsSimple1_42,
                                                                fitSccsModelArgs = fitSccsModelArgs)
  
  sccsAnalysis6 <- SelfControlledCaseSeries::createSccsAnalysis(analysisId = 6,
                                                                description = "SCCS adjusting for age and season, tar 1-42 days",
                                                                getDbSccsDataArgs = getDbSccsDataArgs,
                                                                createStudyPopulationArgs = createStudyPopulationArgs,
                                                                createSccsIntervalDataArgs = createSccsIntervalDataArgsAgeSeason1_42,
                                                                fitSccsModelArgs = fitSccsModelArgs)
  
  sccsAnalysis7 <- SelfControlledCaseSeries::createSccsAnalysis(analysisId = 7,
                                                                description = "SCRI with prior control interval, tar 1-42 days",
                                                                getDbSccsDataArgs = getDbSccsDataArgs,
                                                                createStudyPopulationArgs = createStudyPopulationArgs,
                                                                design = "SCRI",
                                                                createScriIntervalDataArgs = createScriIntervalDataArgs1_42,
                                                                fitSccsModelArgs = fitSccsModelArgs)
  
  sccsAnalysis8 <- SelfControlledCaseSeries::createSccsAnalysis(analysisId = 8,
                                                                description = "SCRI with posterior control interval, tar 1-42 days",
                                                                getDbSccsDataArgs = getDbSccsDataArgs,
                                                                createStudyPopulationArgs = createStudyPopulationArgs,
                                                                design = "SCRI",
                                                                createScriIntervalDataArgs = createScriIntervalDataArgsPost1_42,
                                                                fitSccsModelArgs = fitSccsModelArgs)
  
  
  sccsAnalysis9 <- SelfControlledCaseSeries::createSccsAnalysis(analysisId = 9,
                                                                description = "SCCS, tar 0-1 days",
                                                                getDbSccsDataArgs = getDbSccsDataArgs,
                                                                createStudyPopulationArgs = createStudyPopulationArgs,
                                                                createSccsIntervalDataArgs = createSccsIntervalDataArgsSimple0_1,
                                                                fitSccsModelArgs = fitSccsModelArgs)
  
  sccsAnalysis10 <- SelfControlledCaseSeries::createSccsAnalysis(analysisId = 10,
                                                                description = "SCCS adjusting for age and season, tar 0-1 days",
                                                                getDbSccsDataArgs = getDbSccsDataArgs,
                                                                createStudyPopulationArgs = createStudyPopulationArgs,
                                                                createSccsIntervalDataArgs = createSccsIntervalDataArgsAgeSeason0_1,
                                                                fitSccsModelArgs = fitSccsModelArgs)
  
  sccsAnalysis11 <- SelfControlledCaseSeries::createSccsAnalysis(analysisId = 11,
                                                                description = "SCRI with prior control interval, tar 0-1 days",
                                                                getDbSccsDataArgs = getDbSccsDataArgs,
                                                                createStudyPopulationArgs = createStudyPopulationArgs,
                                                                design = "SCRI",
                                                                createScriIntervalDataArgs = createScriIntervalDataArgs0_1,
                                                                fitSccsModelArgs = fitSccsModelArgs)
  
  sccsAnalysis12 <- SelfControlledCaseSeries::createSccsAnalysis(analysisId = 12,
                                                                description = "SCRI with posterior control interval, tar 0-1 days",
                                                                getDbSccsDataArgs = getDbSccsDataArgs,
                                                                createStudyPopulationArgs = createStudyPopulationArgs,
                                                                design = "SCRI",
                                                                createScriIntervalDataArgs = createScriIntervalDataArgsPost0_1,
                                                                fitSccsModelArgs = fitSccsModelArgs)
  
    
  sccsAnalysisList <- list(sccsAnalysis1, 
                           sccsAnalysis2, 
                           sccsAnalysis3, 
                           sccsAnalysis4, 
                           sccsAnalysis5, 
                           sccsAnalysis6, 
                           sccsAnalysis7, 
                           sccsAnalysis8, 
                           sccsAnalysis9, 
                           sccsAnalysis10, 
                           sccsAnalysis11, 
                           sccsAnalysis12)
  return(sccsAnalysisList)
}
