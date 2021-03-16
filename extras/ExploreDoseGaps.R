library(DatabaseConnector)
library(dplyr)
library(ggplot2)
connection <- connect(connectionDetails)

pathToCsv <- file.path(outputFolder, "AllExposureCohorts.csv")
exposures <- readr::read_csv(pathToCsv, col_types = readr::cols())

exposures <- exposures %>%
  filter(.data$shots == 2 & !.data$sampled & !.data$comparator & .data$shot != "Both")


baseExposureId <- 21198
baseExposureId <- 21198
for (baseExposureId in unique(exposures$baseExposureId)) {
  doses <- exposures %>%
    filter(.data$baseExposureId == !!baseExposureId)
  firstDoseId <- doses %>%
    filter(.data$shot == "First") %>%
    pull(.data$exposureId)
  secondDoseId <- doses %>%
    filter(.data$shot == "Second") %>%
    pull(.data$exposureId)
  
  sql <- "
  SELECT DATEDIFF(DAY, first_dose.cohort_start_date, second_dose.cohort_start_date) AS gap_days,
    COUNT(*) AS gap_count
  FROM @cohort_database_schema.@cohort_table first_dose
  INNER JOIN @cohort_database_schema.@cohort_table second_dose
    ON first_dose.subject_id = second_dose.subject_id
  WHERE first_dose.cohort_definition_id = @first_dose_id
    AND second_dose.cohort_definition_id = @second_dose_id
  GROUP BY DATEDIFF(DAY, first_dose.cohort_start_date, second_dose.cohort_start_date);
  "
  gaps <- renderTranslateQuerySql(connection = connection, 
                                  sql = sql,  
                                  snakeCaseToCamelCase = TRUE,
                                  cohort_database_schema = cohortDatabaseSchema,
                                  cohort_table = cohortTable,
                                  first_dose_id = firstDoseId,
                                  second_dose_id = secondDoseId)
  
  plot <- ggplot(gaps, aes(x = gapDays, y = gapCount)) +
    geom_line() +
    ggtitle(doses$baseExposureName[1])
  ggsave(filename = file.path(outputFolder, sprintf("gaps_e%s_%s.png", baseExposureId, databaseId)),
         plot = plot)
}

disconnect(connection)
