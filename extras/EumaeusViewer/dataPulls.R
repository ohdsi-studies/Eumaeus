loadEntireTable <- function(connection, schema, tableName) {
  tryCatch({
    table <- DatabaseConnector::dbReadTable(connection, 
                                            paste(schema, tableName, sep = "."))
  }, error = function(err) {
    stop("Error reading from ", paste(schema, tableName, sep = "."), ": ", err$message)
  })
  colnames(table) <- SqlRender::snakeCaseToCamelCase(colnames(table))
  return(table)
}


getEstimates <- function(connection, schema, databaseId, exposureId, periodId = NULL, analysisIds, methods, calibrated = FALSE) {
  sql <- sprintf("SELECT * 
    FROM %s.estimate 
    WHERE database_id = '%s'
      AND exposure_id = '%s'
      AND analysis_id IN (%s)",
                 schema,
                 databaseId,
                 exposureId,
                 paste(analysisIds, collapse = ","))
  if (!is.null(periodId)) {
    sql <- paste(sql, sprintf("  AND period_id = '%s'", periodId))
  }
  
  subset <- DatabaseConnector::dbGetQuery(connection, sql)
  colnames(subset) <- SqlRender::snakeCaseToCamelCase(colnames(subset))
  idx <- is.na(subset$logRr) | is.infinite(subset$logRr) | is.na(subset$seLogRr) | is.infinite(subset$seLogRr)
  subset$logRr[idx] <- rep(0, sum(idx))
  subset$seLogRr[idx] <- rep(999, sum(idx))
  subset$ci95Lb[idx] <- rep(0, sum(idx))
  subset$ci95Ub[idx] <- rep(999, sum(idx))
  subset$p[idx] <- 1
  idx <- is.na(subset$calibratedLogRr) | is.infinite(subset$calibratedLogRr) | is.na(subset$calibratedSeLogRr) | is.infinite(subset$calibratedSeLogRr)
  subset$calibratedLogRr[idx] <- rep(0, sum(idx))
  subset$calibratedSeLogRr[idx] <- rep(999, sum(idx))
  subset$calibratedCi95Lb[idx] <- rep(0, sum(idx))
  subset$calibratedCi95Ub [idx] <- rep(999, sum(idx))
  subset$calibratedP[is.na(subset$calibratedP)] <- rep(1, sum(is.na(subset$calibratedP)))
  
  subset <- subset[subset$method %in% methods, ]
  
  if (calibrated) {
    subset$logRr <- subset$calibratedLogRr
    subset$seLogRr <- subset$calibratedSeLogRr
    subset$ci95Lb <- subset$calibratedCi95Lb
    subset$ci95Ub <- subset$calibratedCi95Ub
    subset$p <- subset$calibratedP
  }
  return(subset)
}
