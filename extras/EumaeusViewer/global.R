source("dataPulls.R")

connectionPool <- pool::dbPool(drv = DatabaseConnector::DatabaseConnectorDriver(),
                               dbms = "postgresql",
                               server = paste(keyring::key_get("ohdsiPostgresServer"),
                                              keyring::key_get("ohdsiPostgresShinyDatabase"),
                                              sep = "/"),
                               user = keyring::key_get("ohdsiPostgresUser"),
                               password = keyring::key_get("ohdsiPostgresPassword"))

onStop(function() {
  if (DBI::dbIsValid(connectionPool)) {
    writeLines("Closing database pool")
    pool::poolClose(connectionPool)
  }
})

schema <- "eumaeus"

analysis <- loadEntireTable(connectionPool, schema, "analysis")
database <- loadEntireTable(connectionPool, schema, "database")
exposure <- loadEntireTable(connectionPool, schema, "exposure")
negativeControlOutcome <- loadEntireTable(connectionPool, schema, "negative_control_outcome")
positiveControlOutcome <- loadEntireTable(connectionPool, schema, "positive_control_outcome")
timePeriod <- loadEntireTable(connectionPool, schema, "time_period")

# subset <- getEstimates(connection = connectionPool,
#                        schema = schema,
#                        databaseId = "IBM_MDCD",
#                        exposureId = 21184,
#                        timeAtRisk = "1-28")

trueRrs <- c("Overall", 1, unique(positiveControlOutcome$effectSize))
timeAtRisks <- unique(analysis$timeAtRisk)

# evalTypeInfoHtml <- readChar("evalType.html", file.info("evalType.html")$size)
# calibrationInfoHtml <- readChar("calibration.html", file.info("calibration.html")$size)
# mdrrInfoHtml <- readChar("mdrr.html", file.info("mdrr.html")$size)
# databaseInfoHtml <- readChar("databases.html", file.info("databases.html")$size)
# stratumInfoHtml <- readChar("strata.html", file.info("strata.html")$size)
# trueRrInfoHtml <- readChar("trueRr.html", file.info("trueRr.html")$size)
# methodsInfoHtml <- readChar("methods.html", file.info("methods.html")$size)
# metricInfoHtml <- readChar("metrics.html", file.info("metrics.html")$size)