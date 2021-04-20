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

# Delete NA critical values ------------------------------
cvFiles <- list.files(outputFolder, ".*_withCvs.csv")
for (cvFile in cvFiles) {
  cvs <- readr::read_csv(file.path(outputFolder, cvFile), col_types = readr::cols())  
  idx <- is.na(cvs$criticalValue)
  if (any(idx)) {
    message(sprintf("Removing %d (%0.1f%%) NA values from %s", sum(idx), 100*mean(idx), cvFile))
    cvs <- cvs[!idx, ]  
    readr::write_csv(cvs, file.path(outputFolder, cvFile))
  }
}

# Rerun computation of critical values --------------------------------
options("CvTimeout" = 10)
Eumaeus:::computeCriticalValues(outputFolder, maxCores)

# Insert new CVs in results ------------------------------------------------
library(dplyr)
historicComparatorCvs <- Eumaeus:::loadEstimates(file.path(outputFolder, "hcSummary_withCvs.csv")) %>%
  transmute(.data$analysisId, 
            .data$exposureId, 
            .data$outcomeId, 
            periodId = .data$seqId, 
            method = "HistoricalComparator",
            .data$criticalValue) 

exposures <- Eumaeus:::loadExposureCohorts(outputFolder) 
mapping <- exposures %>%
  filter(.data$sampled == FALSE) %>%
  select(nonSampleExposureId = .data$exposureId, .data$baseExposureId, .data$shot, .data$comparator) %>%
  inner_join(exposures %>%
               filter(.data$sampled == TRUE), 
             by = c("baseExposureId", "shot", "comparator")) %>%
  select(.data$nonSampleExposureId, .data$exposureId) 

cohortMethodCvs <- Eumaeus:::loadEstimates(file.path(outputFolder, "cmSummary_withCvs.csv")) %>%
  inner_join(mapping, by = "exposureId") %>%
  transmute(.data$analysisId,
            exposureId = .data$nonSampleExposureId,
            .data$outcomeId,
            periodId = .data$seqId,
            method = "CohortMethod",
            .data$criticalValue)

caseControlCvs <- Eumaeus:::loadEstimates(file.path(outputFolder, "ccSummary_withCvs.csv")) %>%
  transmute(.data$analysisId, 
            .data$exposureId, 
            .data$outcomeId, 
            periodId = .data$seqId, 
            method = "CaseControl",
            .data$criticalValue) 

sccsCvs <- Eumaeus:::loadEstimates(file.path(outputFolder, "sccsSummary_withCvs.csv")) %>%
  transmute(.data$analysisId, 
            .data$exposureId, 
            .data$outcomeId, 
            periodId = .data$seqId, 
            method = "SCCS",
            .data$criticalValue) 

cvs <- bind_rows(historicComparatorCvs,
          cohortMethodCvs,
          caseControlCvs,
          sccsCvs)
colnames(cvs) <- SqlRender::camelCaseToSnakeCase(colnames(cvs))

estimate <- readr::read_csv(file.path(outputFolder, "export", "estimate.csv"), col_types = readr::cols(), guess_max = 1e5)
before <- nrow(estimate)
estimate <- estimate %>%
  select(-.data$critical_value) %>%
  inner_join(cvs,  by = c("method", "analysis_id", "exposure_id", "outcome_id", "period_id"))
after <- nrow(estimate)
if (before != after) {
  stop("Stop!")
}
readr::write_csv(estimate, file.path(outputFolder, "export", "estimate.csv"))

# Recreate zip file ----------------------------------------
zipName <- normalizePath(file.path(outputFolder, "export", sprintf("Results_%s.zip", databaseId)), mustWork = FALSE)
files <- list.files(file.path(outputFolder, "export"), pattern = ".*\\.csv$")
oldWd <- setwd(file.path(outputFolder, "export"))
DatabaseConnector::createZipFile(zipFile = zipName, files = files)
setwd(oldWd)
