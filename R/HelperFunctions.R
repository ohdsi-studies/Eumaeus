# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of VaccineSurveillanceMethodEvaluation
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
                    endDate = splitDates[2:n],
                    seqId = 1:(n-1),
                    label = sprintf("%d months", 1:(n-1)))
  periods$label[1] <- "1 month"
  return(periods)
}

addCohortNames <- function(data, IdColumnName = "cohortDefinitionId", nameColumnName = "cohortName") {
  cohortsToCreate <- loadCohortsToCreate()
  negativeControls <- loadNegativeControls()
  idToName <- tibble(cohortId = c(cohortsToCreate$cohortId,
                                  negativeControls$outcomeId),
                     cohortName = c(as.character(cohortsToCreate$name),
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
  pathToCsv <- system.file("settings", "CohortsToCreate.csv", package = "VaccineSurveillanceMethodEvaluation")
  cohortsToCreate <- readr::read_csv(pathToCsv, col_types = c(cohortId = "c")) %>%
    mutate(cohortId = bit64::as.integer64(.data$cohortId))
  return(cohortsToCreate)
}

loadNegativeControls <- function() {
  pathToCsv <- system.file("settings", "NegativeControls.csv", package = "VaccineSurveillanceMethodEvaluation")
  negativeControls <- readr::read_csv(pathToCsv, col_types = c(exposureId = "c", outcomeId = "c")) %>%
    mutate(exposureId = bit64::as.integer64(.data$exposureId), outcomeId = bit64::as.integer64(.data$outcomeId))
  return(negativeControls)
}

loadSynthesisSummary <- function(synthesisSummaryFile) {
  synthesisSummary <- readr::read_csv(synthesisSummaryFile, col_types = c(exposureId = "c", outcomeId = "c", newOutcomeId = "c")) %>%
    mutate(exposureId = bit64::as.integer64(.data$exposureId), 
           outcomeId = bit64::as.integer64(.data$outcomeId),
           newOutcomeId = bit64::as.integer64(.data$newOutcomeId))
  return(synthesisSummary)
}

loadAllControls <- function(outputFolder) {
  allControls <- readr::read_csv(file.path(outputFolder , "allControls.csv"), col_types = c(exposureId = "c", outcomeId = "c", oldOutcomeId = "c")) %>%
    mutate(exposureId = bit64::as.integer64(.data$exposureId), 
           outcomeId = bit64::as.integer64(.data$outcomeId),
           oldOutcomeId = bit64::as.integer64(.data$oldOutcomeId))
  return(allControls)
}

loadExposuresofInterest <- function() {
  pathToCsv <- system.file("settings", "ExposuresOfInterest.csv", package = "VaccineSurveillanceMethodEvaluation")
  exposuresOfInterest <- readr::read_csv(pathToCsv, col_types = c(exposureId = "c")) %>%
    mutate(exposureId = bit64::as.integer64(.data$exposureId),
           startDate = as.Date(.data$startDate, format = "%d-%m-%Y"),
           endDate = as.Date(.data$endDate, format = "%d-%m-%Y"),
           historyStartDate = as.Date(.data$historyStartDate, format = "%d-%m-%Y"),
           historyEndDate = as.Date(.data$historyEndDate, format = "%d-%m-%Y"))
  return(exposuresOfInterest)
}

loadEstimates <- function(fileName) {
  estimates <- readr::read_csv(fileName, col_types = c(exposureId = "c", outcomeId = "c")) %>%
    mutate(exposureId = bit64::as.integer64(.data$exposureId), 
           outcomeId = bit64::as.integer64(.data$outcomeId))
  return(estimates)
}
