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

computeCriticalValues <- function(outputFolder, maxCores) {
  cluster <- ParallelLogger::makeCluster(min(10, maxCores))
  ParallelLogger::clusterRequire(cluster, "dplyr")
  on.exit(ParallelLogger::stopCluster(cluster))
  
  computeCvsIncrementally(estimatesFileName = file.path(outputFolder, "hcSummary.csv"), 
                          estimatesWithCvsFileName = file.path(outputFolder, "hcSummary_withCvs.csv"),
                          cvFunction = Eumaeus:::computeHistoricalComparatorCv, 
                          cluster = cluster) 
  
  computeCvsIncrementally(estimatesFileName = file.path(outputFolder, "ccSummary.csv"), 
                          estimatesWithCvsFileName = file.path(outputFolder, "ccSummary_withCvs.csv"),
                          cvFunction = computeCaseControlCv, 
                          cluster = cluster) 
  
  computeCvsIncrementally(estimatesFileName = file.path(outputFolder, "cmSummary.csv"), 
                          estimatesWithCvsFileName = file.path(outputFolder, "cmSummary_withCvs.csv"),
                          cvFunction = computeCohortMethodCv, 
                          cluster = cluster) 
  
  computeCvsIncrementally(estimatesFileName = file.path(outputFolder, "sccsSummary.csv"), 
                          estimatesWithCvsFileName = file.path(outputFolder, "sccsSummary_withCvs.csv"),
                          cvFunction = computeSccsCv, 
                          cluster = cluster) 
}

computeCvsIncrementally <- function(estimatesFileName, estimatesWithCvsFileName, cvFunction, cluster) {
  estimates <- loadEstimates(estimatesFileName) 
  if (file.exists(estimatesWithCvsFileName)) {
    estimatesWithCvs <- loadEstimates(estimatesWithCvsFileName)  
    newEstimates <- estimates %>%
      anti_join(select(estimatesWithCvs, 
                       .data$exposureId, 
                       .data$outcomeId, 
                       .data$seqId, 
                       .data$analysisId,
                       .data$llr),
                by = c("llr", "outcomeId", "analysisId", "exposureId", "seqId"))
    oldEstimatesWithCvs <- estimatesWithCvs %>%
      inner_join(select(estimates, 
                        .data$exposureId, 
                        .data$outcomeId, 
                        .data$seqId, 
                        .data$analysisId,
                        .data$llr),
                 by = c("llr", "outcomeId", "analysisId", "exposureId", "seqId"))
  } else {
    newEstimates <- estimates
    oldEstimatesWithCvs <- tibble()
  }
  if (nrow(newEstimates) > 0) {
    ParallelLogger::logInfo(sprintf("- Computing critical values for %s", basename(estimatesFileName)))
    
    subsets <- split(newEstimates, paste(newEstimates$analysisId, newEstimates$exposureId, newEstimates$outcomeId))
    cvs <- ParallelLogger::clusterApply(cluster, subsets, cvFunction)
    cvs <- bind_rows(cvs)
    rm(subsets)
    newEstimatesWithCvs <- newEstimates %>%
      inner_join(cvs, by = c("analysisId", "exposureId", "outcomeId"))
    estimatesWithCvs <- bind_rows(oldEstimatesWithCvs, newEstimatesWithCvs)
    readr::write_csv(estimatesWithCvs, estimatesWithCvsFileName)
  }
}

computeCaseControlCv <- function(subset) {
  # subset <- subsets[[1]]
  sampleSizeUpperLimit <- max(subset$cases, na.rm = TRUE)
  if (sampleSizeUpperLimit <= 5) {
    cv <- NA
  } else {
    cases <- subset %>%
      arrange(.data$seqId) %>%
      pull(.data$cases)
    looks <- length(cases)
    if (looks > 1) {
      cases[2:looks] <- cases[2:looks] - cases[1:(looks-1)]
      cases <- cases[cases != 0]
    }
    cv <- computeTruncatedBinomialCv(n = sampleSizeUpperLimit,
                                     z = max(subset$controls) / max(subset$cases),
                                     groupSizes = cases)
  }
  return(tibble(analysisId = subset$analysisId[1],
                exposureId = subset$exposureId[1],
                outcomeId = subset$outcomeId[1],
                criticalValue = cv))
}

computeCohortMethodCv <- function(subset) {
  # subset <- subsets[[1]]
  # subset <- cmEstimates[cmEstimates$analysisId == 10 & cmEstimates$exposureId == 211841 & cmEstimates$outcomeId == 10703, ]
  subset$events <- subset$eventsTarget + subset$eventsComparator
  sampleSizeUpperLimit <- max(subset$events, na.rm = TRUE)
  if (sampleSizeUpperLimit <= 5) {
    cv <- NA
  } else {
    events <- subset %>%
      arrange(.data$seqId) %>%
      pull(.data$events)
    looks <- length(events)
    if (looks > 1) {
      events[2:looks] <- events[2:looks] - events[1:(looks-1)]
      events <- events[events > 0]
      sampleSizeUpperLimit <- sum(events)
    }
    cv <- computeTruncatedBinomialCv(n = sampleSizeUpperLimit,
                                     z = max(subset$comparatorDays) / max(subset$targetDays),
                                     groupSizes = events)
  }
  return(tibble(analysisId = subset$analysisId[1],
                exposureId = subset$exposureId[1],
                outcomeId = subset$outcomeId[1],
                criticalValue = cv))
}

computeSccsCv <- function(subset) {
  # subset <- subsets[[1]]
  sampleSizeUpperLimit <- max(subset$outcomeEvents , na.rm = TRUE)
  if (sampleSizeUpperLimit <= 5) {
    cv <- NA
  } else {
    events <- subset %>%
      arrange(.data$seqId) %>%
      pull(.data$outcomeEvents )
    looks <- length(events)
    if (looks > 1) {
      events[2:looks] <- events[2:looks] - events[1:(looks-1)]
      events <- events[events != 0]
    }
    cv <- computeTruncatedBinomialCv(n = sampleSizeUpperLimit,
                                     z = max(subset$daysObserved - subset$exposedDays) / max(subset$exposedDays),
                                     groupSizes = events)
  }
  return(tibble(analysisId = subset$analysisId[1],
                exposureId = subset$exposureId[1],
                outcomeId = subset$outcomeId[1],
                criticalValue = cv))
}

computeHistoricalComparatorCv <- function(subset) {
  # subset <- subsets[[2]]
  expectedOutcomes <- subset %>%
    arrange(.data$seqId) %>%
    pull(.data$expectedOutcomes)
  looks <- length(expectedOutcomes)
  if (looks > 1) {
    expectedOutcomes[2:looks] <- expectedOutcomes[2:looks] - expectedOutcomes[1:(looks-1)]
    # Per-look expected counts < 1 can lead to CV.Poisson() getting stuck in infinite loop, so combining smaller looks:
    eos <- c()
    pending <- 0
    for (eo in expectedOutcomes) {
      if (!is.na(eo)) {
        if (eo + pending >= 1) {
          eos <- c(eos, eo + pending)
          pending <- 0
        } else {
          pending <- eo + pending
        }
      }
    }
    expectedOutcomes <- eos
    sampleSizeUpperLimit <- sum(expectedOutcomes)
  }
  if (sampleSizeUpperLimit <= 5) {
    cv <- NA
  } else {
    cv <- computeTruncatedPoissonCv(n = sampleSizeUpperLimit,
                                    groupSizes = expectedOutcomes)
  }
  return(tibble(analysisId = subset$analysisId[1],
                exposureId = subset$exposureId[1],
                outcomeId = subset$outcomeId[1],
                criticalValue = cv))
}

computeTruncatedBinomialCv <- function(n, z, groupSizes) {
  if (n > 250) {
    groupSizes <- round(groupSizes * 250 / n)
    groupSizes <- groupSizes[groupSizes > 0]
    n <- sum(groupSizes)
  }
  # This check is done inside Sequential::CV.Binomial as well, but will throw an error there:
  pst <- 1/(1 + z)
  if (1 - pbinom(n - 1, n, pst) > 0.05) {
    return(NA)
  }
  cv <- Sequential::CV.Binomial(N = n,
                                M = 1,
                                alpha = 0.05,
                                z = z,
                                GroupSizes = groupSizes)$cv
  return(cv)
}

computeTruncatedPoissonCv <- function(n, groupSizes) {
  if (n > 250) {
    groupSizes <- round(groupSizes * 250 / n)
    groupSizes <- groupSizes[groupSizes > 0]
    n <- sum(groupSizes)
  }
  cv <- Sequential::CV.Poisson(SampleSize = n,
                               alpha = 0.05,
                               M = 1,
                               GroupSizes = groupSizes)
  return(cv)
}
