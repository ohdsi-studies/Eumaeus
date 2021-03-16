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

splitTimePeriod <- function(startDate, endDate) {
  # Split study period into 1 months increments to simulate data accumulation
  
  splitDates <- seq(startDate, endDate, by = paste (1, "months"))
  n <- length(splitDates)
  periods <- tibble(startDate = startDate,
                    endDate = splitDates[2:n] - 1,
                    seqId = 1:(n-1),
                    label = sprintf("%d months", 1:(n-1)))
  periods$label[1] <- "1 month"
  
  lastPeriod <- periods[nrow(periods), ]
  if (lastPeriod$endDate + 1 < endDate) {
    periods <- bind_rows(periods, 
                         tibble(startDate = startDate,
                                endDate = !!endDate,
                                seqId = lastPeriod$seqId + 1,
                                label = sprintf("%d months", lastPeriod$seqId + 1)))
  }
  return(periods)
}

addCohortNames <- function(data, outputFolder, IdColumnName = "cohortDefinitionId", nameColumnName = "cohortName") {
  
  negativeControls <- loadNegativeControls()
  exposureCohorts <- loadExposureCohorts(outputFolder)
  idToName <- tibble(cohortId = c(exposureCohorts$exposureId,
                                  negativeControls$outcomeId),
                     cohortName = c(as.character(exposureCohorts$exposureName),
                                    as.character(negativeControls$outcomeName))) %>%
    distinct(.data$cohortId, .data$cohortName)
  colnames(idToName)[1] <- IdColumnName
  colnames(idToName)[2] <- nameColumnName
  data <- data %>%
    left_join(idToName, by = IdColumnName)
  
  # Change order of columns:
  idCol <- which(colnames(data) == IdColumnName)
  if (idCol < ncol(data) - 1) {
    data <- data[, c(1:idCol, ncol(data) , (idCol+1):(ncol(data)-1))]
  }
  return(data)
}

loadCohortsToCreate <- function() {
  pathToCsv <- system.file("settings", "CohortsToCreate.csv", package = "Eumaeus")
  cohortsToCreate <- readr::read_csv(pathToCsv, col_types = readr::cols())
  return(cohortsToCreate)
}

loadExposureCohorts <- function(outputFolder) {
  pathToCsv <- file.path(outputFolder, "AllExposureCohorts.csv")
  cohortsToCreate <- readr::read_csv(pathToCsv, col_types = readr::cols())
  return(cohortsToCreate)
}

loadNegativeControls <- function() {
  pathToCsv <- system.file("settings", "NegativeControls.csv", package = "Eumaeus")
  negativeControls <- readr::read_csv(pathToCsv, col_types = readr::cols())
  return(negativeControls)
}

loadSynthesisSummary <- function(synthesisSummaryFile) {
  synthesisSummary <- readr::read_csv(synthesisSummaryFile, col_types = readr::cols())
  return(synthesisSummary)
}

loadAllControls <- function(outputFolder) {
  allControls <- readr::read_csv(file.path(outputFolder , "allControls.csv"), col_types = readr::cols())
  return(allControls)
}

loadAdditionalConceptsToExclude <- function(outputFolder) {
  data <- readr::read_csv(file.path(outputFolder , "ToExclude.csv"), col_types = readr::cols())
  return(data)
}

loadExposuresofInterest <- function(exposureIds = NULL) {
  pathToCsv <- system.file("settings", "ExposuresOfInterest.csv", package = "Eumaeus")
  # Excel date compatibility:
  exposuresOfInterest <- readr::read_csv(pathToCsv, col_types = readr::cols()) %>%
    mutate(startDate = as.Date(.data$startDate, format = "%d-%m-%Y"),
           endDate = as.Date(.data$endDate, format = "%d-%m-%Y"),
           historyStartDate = as.Date(.data$historyStartDate, format = "%d-%m-%Y"),
           historyEndDate = as.Date(.data$historyEndDate, format = "%d-%m-%Y"))
  
  if (!is.null(exposureIds)) {
    exposuresOfInterest <- exposuresOfInterest %>%
      filter(.data$exposureId %in% exposureIds)
  }
  return(exposuresOfInterest)
}

loadEstimates <- function(fileName) {
  estimates <- readr::read_csv(fileName, col_types = readr::cols(), guess_max = 1e4)
  return(estimates)
}

loadCmEstimates <- function(fileName) {
  estimates <- readr::read_csv(fileName, col_types = readr::cols())
  return(estimates)
}

