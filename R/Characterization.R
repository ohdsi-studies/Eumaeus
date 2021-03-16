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

createDbCharacterization <- function(connectionDetails,
                                     cdmDatabaseSchema,
                                     outputFolder) {
    start <- Sys.time()
    
    conn <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(conn))
    
    tables <- list()
    
    # Population count ------------------------------------------------------------
    ParallelLogger::logInfo("Counting overall population")
    sql <- "
    SELECT COUNT(*) AS subject_count
    FROM @cdm_database_schema.person;
    "
    popCount <- DatabaseConnector::renderTranslateQuerySql(conn,
                                                           sql,
                                                           cdm_database_schema = cdmDatabaseSchema,
                                                           snakeCaseToCamelCase = TRUE)
    popCount <- popCount %>%
        mutate(stratum = "",
               stratification = "All")
    tables[[length(tables) + 1]] <- popCount
    
    # Observation duration --------------------------------------------------------
    ParallelLogger::logInfo("Counting observation duration")
    sql <- "
    SELECT COUNT(*) AS subject_count,
        observation_years AS stratum
    FROM (
        SELECT ROUND(SUM(DATEDIFF(DAY, observation_period_start_date, observation_period_end_date)) / 365.25, 0) AS observation_years
        FROM @cdm_database_schema.observation_period
        GROUP BY person_id
    ) tmp
    GROUP BY observation_years
    ORDER BY observation_years;
    "
    obsDuration <- DatabaseConnector::renderTranslateQuerySql(conn,
                                                              sql,
                                                              cdm_database_schema = cdmDatabaseSchema,
                                                              snakeCaseToCamelCase = TRUE)
    obsDuration <- obsDuration %>%
        mutate(stratum = as.character(.data$stratum),
               stratification = "Observation years")
    tables[[length(tables) + 1]] <- obsDuration
    
    # Gender ------------------------------------------------------------
    ParallelLogger::logInfo("Counting gender")
    sql <- "
    SELECT subject_count,
        concept_name AS stratum
    FROM (
        SELECT gender_concept_id,
            COUNT(*) AS subject_count
        FROM @cdm_database_schema.person
        GROUP BY gender_concept_id
    ) tmp
    INNER JOIN @cdm_database_schema.concept
        ON gender_concept_id = concept_id
    ORDER BY concept_name;
    "
    gender <- DatabaseConnector::renderTranslateQuerySql(conn,
                                                         sql,
                                                         cdm_database_schema = cdmDatabaseSchema,
                                                         snakeCaseToCamelCase = TRUE)
    gender <- gender %>%
        mutate(stratification = "Gender")
    tables[[length(tables) + 1]] <- gender
    
    # Visit types -----------------------------------------------------
    ParallelLogger::logInfo("Counting visit types")
    sql <- "
    SELECT concept_name AS stratum,
        subject_count
    FROM (
        SELECT visit_concept_id,
            COUNT_BIG(*) AS subject_count
        FROM @cdm_database_schema.visit_occurrence
        GROUP BY visit_concept_id
    ) tmp
    INNER JOIN @cdm_database_schema.concept
        ON visit_concept_id = concept_id
    ORDER BY concept_name;
    "
    visitTypes <- DatabaseConnector::renderTranslateQuerySql(conn,
                                                             sql,
                                                             cdm_database_schema = cdmDatabaseSchema,
                                                             snakeCaseToCamelCase = TRUE)
    visitTypes <- visitTypes %>%
        mutate(stratification = "Visit type")
    tables[[length(tables) + 1]] <- visitTypes
    
    # Age observed ----------------------------------------
    ParallelLogger::logInfo("Counting age observed")
    ages <- tibble(startAge = seq(0, 120, by = 5),
                   endAge = seq(4, 124, by = 5)) %>%
        mutate(ageLabel = sprintf("%d-%d", .data$startAge, .data$endAge))
    DatabaseConnector::insertTable(connection = conn,
                                   tableName = "#ages",
                                   data = ages,
                                   dropTableIfExists = TRUE,
                                   createTable = TRUE,
                                   tempTable = TRUE,
                                   camelCaseToSnakeCase = TRUE)
    
    sql <- "
    SELECT COUNT(DISTINCT person.person_id) AS subject_count,
        age_label AS stratum
    FROM (
        SELECT person_id,
            DATEFROMPARTS(year_of_birth, ISNULL(month_of_birth, 6), ISNULL(day_of_birth, 15)) AS date_of_birth
        FROM @cdm_database_schema.person
    ) person
    CROSS JOIN #ages
    INNER JOIN @cdm_database_schema.observation_period
        ON person.person_id = observation_period.person_id
        AND DATEADD(YEAR, end_age + 1, date_of_birth) >= observation_period_start_date
        AND DATEADD(YEAR, start_age, date_of_birth) <= observation_period_end_date
    GROUP BY age_label,
        start_age
    ORDER BY start_age;
    "
    
    age <- DatabaseConnector::renderTranslateQuerySql(conn,
                                                      sql,
                                                      cdm_database_schema = cdmDatabaseSchema,
                                                      snakeCaseToCamelCase = TRUE)
    age <- age %>%
        mutate(stratification = "Age")
    tables[[length(tables) + 1]] <- age
    DatabaseConnector::renderTranslateExecuteSql(conn, "TRUNCATE TABLE #ages; DROP TABLE #ages", progressBar = FALSE, reportOverallTime = FALSE)
    
    
    # Year observed ----------------------------------------
    ParallelLogger::logInfo("Counting calendar years observed")
    years <- data.frame(calendarYear = 1900:2100)
    DatabaseConnector::insertTable(connection = conn,
                                   tableName = "#years",
                                   data = years,
                                   dropTableIfExists = TRUE,
                                   createTable = TRUE,
                                   tempTable = TRUE,
                                   camelCaseToSnakeCase = TRUE)
    
    sql <- "
    SELECT COUNT(DISTINCT person_id) AS subject_count,
        calendar_year AS stratum
    FROM (
        SELECT calendar_year,
            DATEFROMPARTS(calendar_year, 1, 1) AS year_start,
            DATEFROMPARTS(calendar_year, 12, 31) AS year_end
        FROM #years
    ) years
    INNER JOIN @cdm_database_schema.observation_period
        ON year_end >= observation_period_start_date
            AND year_start <= observation_period_end_date
    GROUP BY calendar_year
    ORDER BY calendar_year;
    "
    
    calenderYears <- DatabaseConnector::renderTranslateQuerySql(conn,
                                                                sql,
                                                                cdm_database_schema = cdmDatabaseSchema,
                                                                snakeCaseToCamelCase = TRUE)
    calenderYears <- calenderYears %>%
        mutate(stratum = as.character(.data$stratum),
               stratification = "Calendar year")
    tables[[length(tables) + 1]] <- calenderYears
    DatabaseConnector::renderTranslateExecuteSql(conn, "TRUNCATE TABLE #years; DROP TABLE #years", progressBar = FALSE, reportOverallTime = FALSE)
    
    
    table <- bind_rows(tables) %>%
        rename(count = .data$subjectCount)
    readr::write_csv(table, file.path(outputFolder, "DbCharacterization.csv"))
    
    delta <- Sys.time() - start
    writeLines(paste("Characterization took", signif(delta, 3), attr(delta, "units")))
}
