CohortDiagnostics::launchCohortExplorer(connectionDetails = connectionDetails,
                                        cdmDatabaseSchema = cdmDatabaseSchema,
                                        cohortDatabaseSchema = cohortDatabaseSchema,
                                        cohortTable = cohortTable,
                                        cohortId = 211844)



connectionDetails1 <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
                                                                connectionString = keyring::key_get("redShiftConnectionStringOhdaMdcd"),
                                                                user = keyring::key_get("redShiftUserName"),
                                                                password = keyring::key_get("redShiftPassword"))
cdmDatabaseSchema1 = "cdm_truven_mdcd_v1476"

connectionDetails2 <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
                                                                connectionString = keyring::key_get("redShiftConnectionStringOhdaOptumEhr"),
                                                                user = keyring::key_get("redShiftUserName"),
                                                                password = keyring::key_get("redShiftPassword"))
cdmDatabaseSchema2 = "cdm_optum_ehr_v1482"

sql <- "SELECT delta_months,
  COUNT(*) AS delta_count
FROM (
  SELECT DATEDIFF(MONTH, observation_period_start_date, MIN(observation_date)) AS delta_months
  FROM @cdm_database_schema.observation
  INNER JOIN @cdm_database_schema.observation_period
    ON observation.person_id = observation_period.person_id
      AND observation_date >= observation_period_start_date
      AND observation_date <= observation_period_end_date
  GROUP BY observation_period_start_date,
    observation.person_id
) tmp
GROUP BY delta_months;"

connection <- connect(connectionDetails1)
countsMdcd <- renderTranslateQuerySql(connection, sql, cdm_database_schema = cdmDatabaseSchema1, snakeCaseToCamelCase = TRUE)
disconnect(connection)

connection <- connect(connectionDetails2)
countsOptumEhr <- renderTranslateQuerySql(connection, sql, cdm_database_schema = cdmDatabaseSchema2, snakeCaseToCamelCase = TRUE)
disconnect(connection)

countsMdcd$database <- "MDCD"
countsMdcd$percent <- 100 * countsMdcd$deltaCount / sum(countsMdcd$deltaCount)
countsOptumEhr$database <- "Optum EHR"
countsOptumEhr$percent <- 100 * countsOptumEhr$deltaCount / sum(countsOptumEhr$deltaCount)
counts <- rbind(countsMdcd, countsOptumEhr)

library(ggplot2)
ggplot(counts, aes(x = deltaMonths , y = percent, group = database, fill = database)) +
  geom_bar(position="dodge", stat = "identity") +
  scale_x_continuous("Months between observation_period_start_date and first observation_date", limits = c(-1,50))



sql <- "SELECT SUM(CAST(days_to_observation AS FLOAT)) / SUM(CAST(days_observed AS FLOAT)) AS fraction_unobserved
FROM (
  SELECT DATEDIFF(DAY, observation_period_start_date, MIN(observation_date)) AS days_to_observation,
    DATEDIFF(DAY, observation_period_start_date, observation_period_end_date) AS days_observed
  FROM @cdm_database_schema.observation
  INNER JOIN @cdm_database_schema.observation_period
    ON observation.person_id = observation_period.person_id
      AND observation_date > observation_period_start_date
      AND observation_date <= observation_period_end_date
  GROUP BY observation_period_start_date,
    observation_period_end_date,
    observation.person_id
) tmp;"

connection <- connect(connectionDetails1)
renderTranslateQuerySql(connection, sql, cdm_database_schema = cdmDatabaseSchema1, snakeCaseToCamelCase = TRUE)
# 0.02877882
disconnect(connection)


connection <- connect(connectionDetails2)
renderTranslateQuerySql(connection, sql, cdm_database_schema = cdmDatabaseSchema2, snakeCaseToCamelCase = TRUE)
# 0.2699234
disconnect(connection)

