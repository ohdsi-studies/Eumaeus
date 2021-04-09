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

runFilteredHistoricalComparator <- function(connectionDetails,
                                            cdmDatabaseSchema,
                                            cohortDatabaseSchema,
                                            cohortTable,
                                            outputFolder,
                                            maxCores) {
  start <- Sys.time()
  hcFolder <- file.path(outputFolder, "historicalComparator")
  hcSummaryFile <- file.path(outputFolder, "hcSummary.csv")
  if (!file.exists(hcSummaryFile)) {
    stop("The (unfiltered) historical comparator method must be executed first")
  }
  hcSummary <- loadEstimates(hcSummaryFile)
  if (all(hcSummary$analysisId < 13)) {
    generateHcDiagnostics(connectionDetails = connectionDetails,
                          cdmDatabaseSchema = cdmDatabaseSchema,
                          cohortDatabaseSchema = cohortDatabaseSchema,
                          cohortTable = cohortTable,
                          outputFolder = outputFolder,
                          maxCores = maxCores)
    monthlyRates <- readr::read_csv(file.path(outputFolder, "hcDiagnosticsRates.csv"), col_types = readr::cols())
    exposure <- loadExposureCohorts(outputFolder)
    subsets <- split(hcSummary, paste(hcSummary$exposureId, hcSummary$seqId))
    filteredHcSummary <- purrr::map_dfr(subsets, filterHc, monthlyRates = monthlyRates, exposure = exposure)
    hcSummary <- bind_rows(hcSummary, filteredHcSummary)
    readr::write_csv(hcSummary, hcSummaryFile)
  }
  delta <- Sys.time() - start
  message(paste("Completing all filtered historical comparator analyses took", signif(delta, 3), attr(delta, "units")))
}
# subset <- subsets[[1]]
# subset <- hcSummary[hcSummary$exposureId == 211831 & hcSummary$seqId == 12, ]
filterHc <- function(subset, monthlyRates, exposure) {
  exposureId <- subset$exposureId[1]
  seqId <- subset$seqId[1]
  dateRanges <- exposure %>%
    filter(.data$exposureId == !!exposureId) %>%
    select(.data$historyStartDate, .data$historyEndDate, .data$startDate, .data$endDate) 
  endDate <- splitTimePeriod(startDate = dateRanges$startDate, endDate = dateRanges$endDate) %>%
    filter(.data$seqId == !!seqId) %>%
    pull(.data$endDate)
  
  # Filter if change > 50%
  threshold <- 0.5
  
  monthlyRatesSubset <- monthlyRates %>%
    filter(.data$outcomeId %in% unique(subset$outcomeId))
  historicIr <- monthlyRatesSubset %>%
    filter(.data$endDate > dateRanges$historyStartDate & .data$startDate < dateRanges$historyEndDate) %>%
    group_by(.data$outcomeId) %>%
    summarise(historicIr = sum(.data$cohortCount) / sum(.data$personYears))
  currentIr <- monthlyRatesSubset %>%
    filter(.data$endDate > dateRanges$startDate & .data$startDate < !!endDate) %>%
    group_by(.data$outcomeId) %>%
    summarise(currentIr = sum(.data$cohortCount) / sum(.data$personYears))
  outcomeIds <- inner_join(historicIr, currentIr, by = "outcomeId") %>%
    mutate(delta = abs((.data$currentIr - .data$historicIr) / .data$historicIr)) %>%
    filter(.data$delta > threshold) %>%
    pull(.data$outcomeId)
  
  if (length(outcomeIds) > 0) {
    idx <- subset$outcomeId %in% outcomeIds
    ParallelLogger::logInfo(sprintf("Filtering %d of %d estimates for exposure %d and period %d", sum(idx), length(idx), exposureId, seqId))
    subset$irr[idx] <- NA
    subset$lb95Ci[idx] <- NA
    subset$ub95Ci[idx] <- NA
    subset$logRr[idx] <- NA
    subset$seLogRr[idx] <- NA
    subset$llr[idx] <- NA
  }
  subset <- subset %>%
    mutate(analysisId = .data$analysisId + 12)
  return(subset)
}