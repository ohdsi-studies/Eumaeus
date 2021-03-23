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

compareCohorts <- function(connectionDetails,
                           cdmDatabaseSchema,
                           cohortDatabaseSchema,
                           cohortTable,
                           outputFolder) {
  cohortsToCompare <- loadExposureCohorts(outputFolder) %>%
    filter(.data$sampled == TRUE & (is.na(.data$comparatorType) | grepl("age-sex stratified", .data$comparatorType)))
  
  covariateData <- FeatureExtraction::getDbCovariateData(connectionDetails = connectionDetails,
                                                         cdmDatabaseSchema = cdmDatabaseSchema,
                                                         cohortDatabaseSchema = cohortDatabaseSchema,
                                                         cohortTable = cohortTable,
                                                         cohortTableIsTemp = FALSE,
                                                         cohortId = cohortsToCompare$exposureId,
                                                         covariateSettings = FeatureExtraction::createDefaultCovariateSettings(),
                                                         aggregated = TRUE)
  
  populationSize <- attr(x = covariateData, which = "metaData")$populationSize
  populationSize <- dplyr::tibble(cohortId = names(populationSize),
                                  populationSize = populationSize)
  
  covariates <- covariateData$covariates %>% 
    dplyr::rename(cohortId = .data$cohortDefinitionId) %>% 
    dplyr::left_join(populationSize, by = "cohortId", copy = TRUE) %>% 
    dplyr::mutate(sd = sqrt(((populationSize * .data$sumValue) + .data$sumValue)/(populationSize^2))) %>% 
    dplyr::rename(mean = .data$averageValue) %>% 
    dplyr::select(-.data$sumValue, -.data$populationSize)
  
  covariatesContinuous <- covariateData$covariatesContinuous %>% 
    dplyr::rename(mean = .data$averageValue, 
                  sd = .data$standardDeviation, 
                  cohortId = .data$cohortDefinitionId) %>% 
    dplyr::select(.data$cohortId, .data$covariateId, .data$mean, .data$sd)
  
  covariates <- bind_rows(collect(covariates), collect(covariatesContinuous))
  saveRDS(covariates, file.path(outputFolder, "covariates.rds"))
  
  covariateRef <- collect(covariateData$covariateRef)
  saveRDS(covariateRef, file.path(outputFolder, "covariateRef.rds"))
  
  # Compute SDM
  cohortsToCompare <- loadExposureCohorts(outputFolder) %>%
    filter(.data$sampled == TRUE & (is.na(.data$comparatorType) | grepl("age-sex stratified", .data$comparatorType))) %>%
    select(.data$exposureId, .data$exposureName, .data$baseExposureId, .data$shot, .data$comparator) 
  
  comparisons <- inner_join(filter(cohortsToCompare, .data$comparator == FALSE),
                            filter(cohortsToCompare, .data$comparator == TRUE),
                            by = c("baseExposureId", "shot"),
                            suffix = c("1", "2"))
  
  compareCovariates <- function(comparison, covariates, covariateRef) {
    characteristics1 <- covariates %>%
      filter(.data$cohortId == comparison$exposureId1) %>%
      select(-.data$cohortId)
    characteristics2 <- covariates %>%
      filter(.data$cohortId == comparison$exposureId2) %>%
      select(-.data$cohortId)
    unbalanced <- full_join(x = characteristics1, 
                            y = characteristics2, 
                            by = c("covariateId"),
                            suffix = c("1", "2")) %>%
      mutate(sd = sqrt(.data$sd1^2 + .data$sd2^2),
             stdDiff = (.data$mean2 - .data$mean1)/.data$sd) %>% 
      filter((abs(.data$stdDiff) > 0.01 | is.na(.data$stdDiff)) &
               (.data$mean1 > 0.001 | .data$mean2 > 0.001))
    unbalanced$exposureId1 <- comparison$exposureId1
    unbalanced$exposureName1 <- comparison$exposureName1
    unbalanced$exposureId2 <- comparison$exposureId2
    unbalanced$exposureName2 <- comparison$exposureName2
    unbalanced <- inner_join(unbalanced,
                             select(covariateRef, .data$covariateId, .data$covariateName, .data$conceptId),
                             by = "covariateId") %>%
      arrange(-abs(.data$stdDiff))
    return(unbalanced)
  }
  unbalanced <- purrr::map_dfr(split(comparisons, 1:nrow(comparisons)), compareCovariates, covariates = covariates, covariateRef = covariateRef)
  readr::write_csv(unbalanced, file.path(outputFolder, "Unbalanced.csv"))
  # unbalanced <- readr::read_csv(file.path(outputFolder, "Unbalanced.csv"))
  toExclude <- unbalanced %>%
    filter(.data$stdDiff < 0) %>%
    filter(grepl("immun|vacc|prevent|virus|antibody|antigen|procedure$|classification$|service$", .data$covariateName, ignore.case = TRUE)) %>%
    arrange(-abs(.data$stdDiff))
  readr::write_csv(toExclude, file.path(outputFolder, "ToExclude.csv"))
}