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
            exposureId <- comparison$exposureId1[1]
            exposureFolder <- file.path(cmFolder, sprintf("e_%s", exposureId))
            if (!file.exists(exposureFolder))
                dir.create(exposureFolder)
            
            # Sample target and comparator cohorts -----------------------------------
            cohortCountsFile <- file.path(exposureFolder, "cohortCounts.csv")
            if (!file.exists(cohortCountsFile)) {
                ParallelLogger::logInfo(sprintf("Sampling target and comparator cohorts for exposure %s", exposureId))
                sampleCohorts(connectionDetails = connectionDetails,
                              cdmDatabaseSchema = cdmDatabaseSchema,
                              cohortDatabaseSchema = cohortDatabaseSchema,
                              cohortTable = cohortTable,
                              exposureId = exposureId,
                              targetSampleCohortId = exposureId * 100 + 1,
                              comparatorSampleCohortId = exposureId * 100 + 2,
                              startDate = controls$startDate[1],
                              endDate = controls$endDate[1],
                              cohortCountsFile = cohortCountsFile)
            }
            
            # Create one big CohortMethodData object ----------------------------
            bigCmDataFile <- file.path(exposureFolder, "CmData.zip")
            if (!file.exists(bigCmDataFile)) {
                ParallelLogger::logInfo(sprintf("Constructing CohortMethodData object for exposure %s", exposureId))
                excludedCovariateConceptIds <- loadExposuresofInterest() %>%
                    filter(.data$exposureId == !!exposureId) %>%
                    pull(.data$conceptIdsToExclude) %>%
                    strsplit(";")
                excludedCovariateConceptIds <- as.numeric(excludedCovariateConceptIds[[1]])
                
                covariateSettings <- FeatureExtraction::createDefaultCovariateSettings(excludedCovariateConceptIds = excludedCovariateConceptIds,
                                                                                       addDescendantsToExclude = TRUE)
                bigCmData <- CohortMethod::getDbCohortMethodData(connectionDetails = connectionDetails,
                                                                 cdmDatabaseSchema = cdmDatabaseSchema,
                                                                 exposureDatabaseSchema = cohortDatabaseSchema,
                                                                 exposureTable = cohortTable,
                                                                 outcomeDatabaseSchema = cohortDatabaseSchema,
                                                                 outcomeTable = cohortTable,
                                                                 targetId = exposureId * 100 + 1,
                                                                 comparatorId = exposureId * 100 + 2,
                                                                 outcomeIds = controls$outcomeId,
                                                                 firstExposureOnly = TRUE,
                                                                 washoutPeriod = 365,
                                                                 covariateSettings = covariateSettings)
                
                CohortMethod::saveCohortMethodData(bigCmData, bigCmDataFile)
            } 
            bigCmData <- NULL
            timePeriods <- splitTimePeriod(startDate = controls$startDate[1], endDate = controls$endDate[1])
            # i <- 5
            for (i in 1:nrow(timePeriods)) {
                periodEstimatesFile <- file.path(exposureFolder, sprintf("estimates_t%d.csv", timePeriods$seqId[i]))
                if (!file.exists(periodEstimatesFile)) {
                    ParallelLogger::logInfo(sprintf("Executing cohort method for exposure %s and period: %s", exposureId, timePeriods$label[i]))
                    periodFolder <- file.path(exposureFolder, sprintf("cmOutput_t%d", timePeriods$seqId[i]))
                    if (!file.exists(periodFolder))
                        dir.create(periodFolder)
                    
                    cmDataFile <- file.path(periodFolder, sprintf("CmData_l1_t%s_c%s.zip", exposureId * 100 + 1, exposureId * 100 + 2))
                    if (!file.exists(cmDataFile)) {
                        ParallelLogger::logInfo("- Subsetting CohortMethodData to period")
                        if (is.null(bigCmData)) {
                            bigCmData <- CohortMethod::loadCohortMethodData(bigCmDataFile)
                        }
                        subsetCmData(bigCmData = bigCmData,
                                     endDate = timePeriods$endDate[i],
                                     cmDataFile = cmDataFile)
                        
                    }
                    estimates <- computeCohortMethodEstimates(targetId = exposureId * 100 + 1,
                                                              comparatorId = exposureId * 100 + 2,
                                                              outcomeIds = controls$outcomeId,
                                                              periodFolder = periodFolder)
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
                if (is.null(bigCmData)) {
                    bigCmData <- CohortMethod::loadCohortMethodData(bigCmDataFile)
                }
                omr <- readRDS(file.path(exposureFolder, sprintf("cmOutput_t%d", timePeriods$seqId[nrow(timePeriods)]), "outcomeModelReference.rds"))
                ps <- readRDS(file.path(exposureFolder, sprintf("cmOutput_t%d", timePeriods$seqId[nrow(timePeriods)]), omr$sharedPsFile[omr$sharedPsFile != ""][1]))
                
                # model <- CohortMethod::getPsModel(ps, bigCmData)
                model <- getPsModel(ps, bigCmData)
                readr::write_csv(model, modelFile)
            }
        }
        allEstimates <- bind_rows(allEstimates)  
        allEstimates <- allEstimates %>%
            filter(.data$eventsComparator > 0) %>%
            mutate(exposureId = bit64::as.integer64(.data$targetId/100),
                   expectedOutcomes = .data$targetDays * (.data$eventsComparator / .data$comparatorDays)) %>%
            mutate(llr = llr(.data$eventsTarget, .data$expectedOutcomes))
        readr::write_csv(allEstimates, cmSummaryFile)
    }
    delta <- Sys.time() - start
    writeLines(paste("Completed cohort method analyses in", signif(delta, 3), attr(delta, "units")))
    
    analysisDesc <- tibble(analysisId = c(1, 
                                          2),
                           description = c("Crude cohort method",
                                           "1-on-1 matching"))
    readr::write_csv(analysisDesc, file.path(outputFolder, "cmAnalysisDesc.csv"))
}



sampleCohorts <- function(connectionDetails,
                          cdmDatabaseSchema,
                          cohortDatabaseSchema,
                          cohortTable,
                          targetSampleCohortId,
                          comparatorSampleCohortId,
                          exposureId,
                          startDate,
                          endDate,
                          cohortCountsFile) {
    start <- Sys.time()
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
    
    # sql <- SqlRender::loadRenderTranslateSql("ComputeExposureCountPerMonth.sql",
    #                                          "Eumaeus",
    #                                          dbms = connectionDetails$dbms,
    #                                          cdm_database_schema = cdmDatabaseSchema,
    #                                          cohort_database_schema = cohortDatabaseSchema,
    #                                          cohort_table = cohortTable,
    #                                          exposure_id = exposureId,
    #                                          start_date = format(startDate, "%Y%m%d"),
    #                                          end_date = format(endDate, "%Y%m%d"),
    #                                          washout_period = 365)
    # counts <- DatabaseConnector::querySql(connection = connection,
    #                                       sql = sql,
    #                                       snakeCaseToCamelCase = TRUE)
    # monthlySampleSize <- ceiling(quantile(counts$cohortCount, 0.9) / 10000) * 10000
    # ParallelLogger::logInfo(sprintf("Sampling %d comparator subjects per month", monthlySampleSize))
    
    sql <- SqlRender::loadRenderTranslateSql("SampleComparators.sql",
                                             "Eumaeus",
                                             dbms = connectionDetails$dbms,
                                             cdm_database_schema = cdmDatabaseSchema,
                                             cohort_database_schema = cohortDatabaseSchema,
                                             cohort_table = cohortTable,
                                             exposure_id = exposureId,
                                             target_sample_cohort_id = targetSampleCohortId,
                                             comparator_sample_cohort_id = comparatorSampleCohortId,
                                             start_date = format(startDate, "%Y%m%d"),
                                             end_date = format(endDate, "%Y%m%d"),
                                             washout_period = 365,
                                             multiplier = 2,
                                             max_target_per_month = 350000,
                                             visit_concept_ids = c(9202))
    # sql <- SqlRender::readSql("inst/sql/sql_server/SampleComparators.sql")
    # sql <- SqlRender::render(sql, cdm_database_schema = cdmDatabaseSchema,
    #                          cohort_database_schema = cohortDatabaseSchema,
    #                          cohort_table = cohortTable,
    #                          target_sample_cohort_id = targetSampleCohortId,
    #                          comparator_sample_cohort_id = comparatorSampleCohortId,
    #                          exposure_id = exposureId,
    #                          start_date = format(startDate, "%Y%m%d"),
    #                          end_date = format(endDate, "%Y%m%d"),
    #                          washout_period = 365,
    #                          multiplier = 2,
    #                          max_target_per_month = 350000,
    #                          visit_concept_ids = c(9202))
    # sql <- SqlRender::translate(sql, connectionDetails$dbms)
    
    DatabaseConnector::executeSql(connection, sql)
    
    sql <- "SELECT cohort_definition_id,
            COUNT(*) AS cohort_count,
            YEAR(cohort_start_date) AS visit_year,
            MONTH(cohort_start_date) AS visit_month
        FROM @cohort_database_schema.@cohort_table
        WHERE cohort_definition_id IN (@target_sample_cohort_id, @comparator_sample_cohort_id)
        GROUP BY cohort_definition_id,
            YEAR(cohort_start_date),
            MONTH(cohort_start_date)
        ORDER BY YEAR(cohort_start_date),
            MONTH(cohort_start_date),
            cohort_definition_id;"
    cohortCounts <- DatabaseConnector::renderTranslateQuerySql(connection = connection, 
                                                                         sql = sql, 
                                                                         cohort_database_schema = cohortDatabaseSchema,
                                                                         cohort_table = cohortTable,
                                                                         target_sample_cohort_id = targetSampleCohortId,
                                                                         comparator_sample_cohort_id = comparatorSampleCohortId,
                                                                         snakeCaseToCamelCase = TRUE)
    readr::write_csv(cohortCounts, cohortCountsFile)
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste("Sampling comparator cohort took", signif(delta, 3), attr(delta, "units")))
}

subsetCmData <- function(bigCmData,
                         endDate,
                         cmDataFile) {
    endDate <- as.integer(endDate)
    
    cmData <- Andromeda::andromeda()
    cmData$cohorts <- bigCmData$cohorts %>%
        filter(.data$cohortStartDate < endDate) %>%
        mutate(ifelse(.data$daysToObsEnd > endDate - .data$cohortStartDate, endDate - .data$cohortStartDate, .data$daysToObsEnd))
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
    CohortMethod::saveCohortMethodData(cmData, cmDataFile)
}

computeCohortMethodEstimates <- function(targetId,
                                         comparatorId,
                                         outcomeIds,
                                         periodFolder) {
    
    getDbCmDataArgs <- CohortMethod::createGetDbCohortMethodDataArgs(covariateSettings = NULL)
    
    createStudyPopArgs <- CohortMethod::createCreateStudyPopulationArgs(removeSubjectsWithPriorOutcome = TRUE,
                                                                        removeDuplicateSubjects = TRUE,
                                                                        minDaysAtRisk = 1,
                                                                        riskWindowStart = 1,
                                                                        startAnchor = "cohort start",
                                                                        riskWindowEnd = 28,
                                                                        endAnchor = "cohort start",)
    
    fitOutcomeModelArgs <- CohortMethod::createFitOutcomeModelArgs(useCovariates = FALSE,
                                                                   modelType = "cox",
                                                                   stratified = FALSE)
    
    cmAnalysis1 <- CohortMethod::createCmAnalysis(analysisId = 1,
                                                  description = "Crude cohort method",
                                                  getDbCohortMethodDataArgs = getDbCmDataArgs,
                                                  createStudyPopArgs = createStudyPopArgs,
                                                  fitOutcomeModel = TRUE,
                                                  fitOutcomeModelArgs = fitOutcomeModelArgs)
    
    createPsArgs <- CohortMethod::createCreatePsArgs(errorOnHighCorrelation = TRUE,
                                                     stopOnError = FALSE,
                                                     maxCohortSizeForFitting = 150000,
                                                     control = Cyclops::createControl(cvType = "auto",
                                                                                      startingVariance = 0.01,
                                                                                      noiseLevel = "quiet",
                                                                                      tolerance  = 2e-07,
                                                                                      fold = 10,
                                                                                      cvRepetitions = 1))
    
    matchOnPsArgs <- CohortMethod::createMatchOnPsArgs(maxRatio = 1)
    
    cmAnalysis2 <- CohortMethod::createCmAnalysis(analysisId = 2,
                                                  description = "1-on-1 matching",
                                                  getDbCohortMethodDataArgs = getDbCmDataArgs,
                                                  createStudyPopArgs = createStudyPopArgs,
                                                  createPs = TRUE,
                                                  createPsArgs = createPsArgs,
                                                  matchOnPs = TRUE,
                                                  matchOnPsArgs = matchOnPsArgs,
                                                  fitOutcomeModel = TRUE,
                                                  fitOutcomeModelArgs = fitOutcomeModelArgs)
    
    cmAnalysisList <- list(cmAnalysis1, cmAnalysis2)
    
    tcosList <- list()
    tcosList[[1]] <- CohortMethod::createTargetComparatorOutcomes(targetId = targetId,
                                                                  comparatorId = comparatorId,
                                                                  outcomeIds = outcomeIds)
    
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
                                            fitOutcomeModelThreads = min(max(1, floor(maxCores/8)), 3),
                                            outcomeCvThreads = min(10, maxCores))
    
    estimates <- CohortMethod::summarizeAnalyses(cmResult, periodFolder)
    return(estimates)
}
