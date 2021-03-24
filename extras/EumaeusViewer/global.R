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

loadResultsTable <- function(tableName) {
  tryCatch({
    table <- DatabaseConnector::dbReadTable(connectionPool, 
                                            paste(schema, tableName, sep = "."))
  }, error = function(err) {
    stop("Error reading from ", paste(schema, tableName, sep = "."), ": ", err$message)
  })
  colnames(table) <- SqlRender::snakeCaseToCamelCase(colnames(table))
  if (nrow(table) > 0) {
    assign(SqlRender::snakeCaseToCamelCase(tableName), dplyr::as_tibble(table), envir = .GlobalEnv)
  }
}

loadResultsTable("analysis")
loadResultsTable("database")
loadResultsTable("exposure")
loadResultsTable("negative_control_outcome")
loadResultsTable("positive_control_outcome")
loadResultsTable("time_period")

trueRrs <- c("Overall", ">1", 1, unique(positiveControlOutcome$effectSize))
timeAtRisks <- unique(analysis$timeAtRisk)

# evalTypeInfoHtml <- readChar("evalType.html", file.info("evalType.html")$size)
# calibrationInfoHtml <- readChar("calibration.html", file.info("calibration.html")$size)
# mdrrInfoHtml <- readChar("mdrr.html", file.info("mdrr.html")$size)
# databaseInfoHtml <- readChar("databases.html", file.info("databases.html")$size)
# stratumInfoHtml <- readChar("strata.html", file.info("strata.html")$size)
# trueRrInfoHtml <- readChar("trueRr.html", file.info("trueRr.html")$size)
# methodsInfoHtml <- readChar("methods.html", file.info("methods.html")$size)
# metricInfoHtml <- readChar("metrics.html", file.info("metrics.html")$size)