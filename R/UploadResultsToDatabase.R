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

#' Get specifications for the Eumaeus results data model
#'
#' @return
#' A tibble data frame object with specifications
#'
#' @export
getResultsDataModelSpecifications <- function() {
  pathToCsv <- system.file("settings", "resultsModelSpecs.csv", package = "Eumaeus")
  resultsDataModelSpecifications <- readr::read_csv(file = pathToCsv, col_types = readr::cols())
  return(resultsDataModelSpecifications)
}


checkColumnNames <- function(table, tableName, zipFileName, specifications = getResultsDataModelSpecifications()) {
  observeredNames <- colnames(table)[order(colnames(table))]
  
  tableSpecs <- specifications %>%
    dplyr::filter(.data$tableName == !!tableName)
  
  optionalNames <- dplyr::tibble(fieldName = "")
  
  expectedNames <- tableSpecs %>%
    dplyr::select(.data$fieldName) %>%
    dplyr::anti_join(dplyr::filter(optionalNames, !.data$fieldName %in% observeredNames), by = "fieldName") %>%
    dplyr::arrange(.data$fieldName) %>%
    dplyr::pull()
  
  if (!isTRUE(all.equal(expectedNames, observeredNames))) {
    stop(sprintf("Column names of table %s in zip file %s do not match specifications.\n- Observed columns: %s\n- Expected columns: %s",
                 tableName,
                 zipFileName,
                 paste(observeredNames, collapse = ", "),
                 paste(expectedNames, collapse = ", ")))
  }
}

checkAndFixDataTypes <- function(table, tableName, zipFileName, specifications = getResultsDataModelSpecifications()) {
  tableSpecs <- specifications %>%
    filter(.data$tableName == !!tableName)
  
  observedTypes <- sapply(table, class)
  for (i in 1:length(observedTypes)) {
    fieldName <- names(observedTypes)[i]
    expectedType <- gsub("\\(.*\\)", "", tolower(tableSpecs$type[tableSpecs$fieldName == fieldName]))
    if (expectedType == "bigint" || expectedType == "numeric") {
      if (observedTypes[i] != "numeric" && observedTypes[i] != "double") {
        ParallelLogger::logDebug(sprintf("Field %s in table %s in zip file %s is of type %s, but was expecting %s. Attempting to convert.",
                                         fieldName,
                                         tableName,
                                         zipFileName,
                                         observedTypes[i],
                                         expectedType))
        table <- mutate_at(table, i, as.numeric)
      }
      table <- mutate_at(table, i, infiniteToNa)
    } else if (expectedType == "integer") {
      if (observedTypes[i] != "integer") {
        ParallelLogger::logDebug(sprintf("Field %s in table %s in zip file %s is of type %s, but was expecting %s. Attempting to convert.",
                                         fieldName,
                                         tableName,
                                         zipFileName,
                                         observedTypes[i],
                                         expectedType))
        table <- mutate_at(table, i, as.integer)
      }
    } else if (expectedType == "varchar") {
      if (observedTypes[i] != "character") {
        ParallelLogger::logDebug(sprintf("Field %s in table %s in zip file %s is of type %s, but was expecting %s. Attempting to convert.",
                                         fieldName,
                                         tableName,
                                         zipFileName,
                                         observedTypes[i],
                                         expectedType))
        table <- mutate_at(table, i, as.character)
      }
    } else if (expectedType == "date") {
      if (observedTypes[i] != "Date") {
        ParallelLogger::logDebug(sprintf("Field %s in table %s in zip file %s is of type %s, but was expecting %s. Attempting to convert.",
                                         fieldName,
                                         tableName,
                                         zipFileName,
                                         observedTypes[i],
                                         expectedType))
        table <- mutate_at(table, i, as.Date)
      }
    }
  }
  return(table)
}

infiniteToNa <- function(x) {
  x[is.infinite(x)] <- NA
  return(x)
}

checkAndFixDuplicateRows <- function(table, tableName, zipFileName, specifications = getResultsDataModelSpecifications()) {
  primaryKeys <- specifications %>%
    dplyr::filter(.data$tableName == !!tableName & .data$primaryKey == "Yes") %>%
    dplyr::select(.data$fieldName) %>%
    dplyr::pull()
  duplicatedRows <- duplicated(table[, primaryKeys])
  if (any(duplicatedRows)) {
    warning(sprintf("Table %s in zip file %s has duplicate rows. Removing %s records.",
                    tableName,
                    zipFileName,
                    sum(duplicatedRows)))
    return(table[!duplicatedRows, ])
  } else {
    return(table)
  }
}

appendNewRows <- function(data, newData, tableName, specifications = getResultsDataModelSpecifications()) {
  if (nrow(data) > 0) {
    primaryKeys <- specifications %>%
      dplyr::filter(.data$tableName == !!tableName & .data$primaryKey == "Yes") %>%
      dplyr::select(.data$fieldName) %>%
      dplyr::pull()
    newData <- newData %>%
      dplyr::anti_join(data, by = primaryKeys)
  }
  return(dplyr::bind_rows(data, newData))
}


#' Create the results data model tables on a database server.
#'
#' @details
#' Only PostgreSQL servers are supported.
#'
#' @param connectionDetails   An object of type \code{connectionDetails} as created using the
#'                            \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                            DatabaseConnector package.
#' @param schema         The schema on the postgres server where the tables will be created.
#'
#' @export
createResultsDataModel <- function(connectionDetails, schema) {
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  schemas <- unlist(DatabaseConnector::querySql(connection,
                                                "SELECT schema_name FROM information_schema.schemata;",
                                                snakeCaseToCamelCase = TRUE)[, 1])
  if (!tolower(schema) %in% tolower(schemas)) {
    stop("Schema '", schema, "' not found on database. Only found these schemas: '", paste(schemas, collapse = "', '"), "'")
  }
  DatabaseConnector::executeSql(connection, sprintf("SET search_path TO %s;", schema), progressBar = FALSE, reportOverallTime = FALSE)
  pathToSql <- system.file("sql", "postgresql", "CreateResultsTables.sql", package = "Eumaeus")
  # pathToSql <- file.path("inst", "sql", "postgresql", "CreateResultsTables.sql")
  sql <- SqlRender::readSql(pathToSql)
  DatabaseConnector::executeSql(connection, sql)
}


naToEmpty <- function(x) {
  if (is.character(x)) {
    x[is.na(x)] <- ""
  } else {
    x[is.na(x)] <- -1
  }
  return(x)
}

#' Upload results to the database server.
#'
#' @description
#' Set the POSTGRES_PATH environmental variable to the path to the folder containing the psql executable to enable
#' bulk upload (recommended).
#'
#' @param connectionDetails   An object of type \code{connectionDetails} as created using the
#'                            \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                            DatabaseConnector package.
#' @param schema         The schema on the postgres server where the tables will be created.
#' @param zipFileName    The name of the zip file.
#' @param forceOverWriteOfSpecifications  If TRUE, specifications of the phenotypes, cohort definitions, and analysis
#'                       will be overwritten if they already exist on the database. Only use this if these specifications
#'                       have changed since the last upload.
#' @param purgeSiteDataBeforeUploading If TRUE, before inserting data for a specific databaseId all the data for
#'                       that site will be dropped. This assumes the input zip file contains the full data for that
#'                       data site.
#' @param tempFolder     A folder on the local file system where the zip files are extracted to. Will be cleaned
#'                       up when the function is finished. Can be used to specify a temp folder on a drive that
#'                       has sufficent space if the default system temp space is too limited.
#'
#' @export
uploadResultsToDatabase <- function(connectionDetails = NULL,
                                    schema,
                                    zipFileName,
                                    forceOverWriteOfSpecifications = FALSE,
                                    purgeSiteDataBeforeUploading = TRUE,
                                    tempFolder = tempdir()) {
  start <- Sys.time()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  unzipFolder <- tempfile("unzipTempFolder", tmpdir = tempFolder)
  dir.create(path = unzipFolder, recursive = TRUE)
  on.exit(unlink(unzipFolder, recursive = TRUE), add = TRUE)
  
  ParallelLogger::logInfo("Unzipping ", zipFileName)
  zip::unzip(zipFileName, exdir = unzipFolder)
  
  specifications = getResultsDataModelSpecifications()
  
  if (purgeSiteDataBeforeUploading) {
    database <- readr::read_csv(file = file.path(unzipFolder, "database.csv"), col_types = readr::cols())
    colnames(database) <- SqlRender::snakeCaseToCamelCase(colnames(database))
    databaseId <- database$databaseId
  }
  
  uploadTable <- function(file) {
    tableName <- gsub(".csv", "", file)
    ParallelLogger::logInfo("Uploading table ", tableName)
    
    primaryKey <- specifications %>%
      filter(.data$tableName == !!tableName & .data$primaryKey == "Yes") %>%
      select(.data$fieldName) %>%
      pull()
    
    if (purgeSiteDataBeforeUploading && "database_id" %in% primaryKey) {
      deleteAllRecordsForDatabaseId(connection = connection,
                                    schema = schema,
                                    tableName = tableName,
                                    databaseId = databaseId)
    }
    env <- new.env()
    env$schema <- schema
    env$tableName <- tableName
    env$primaryKey <- primaryKey
    if (purgeSiteDataBeforeUploading && "database_id" %in% primaryKey) {
      env$primaryKeyValuesInDb <- NULL
    } else {
      sql <- "SELECT DISTINCT @primary_key FROM @schema.@table_name;"
      sql <- SqlRender::render(sql = sql,
                               primary_key = primaryKey,
                               schema = schema,
                               table_name = tableName)
      primaryKeyValuesInDb <- DatabaseConnector::querySql(connection, sql)
      colnames(primaryKeyValuesInDb) <- tolower(colnames(primaryKeyValuesInDb))
      env$primaryKeyValuesInDb <- primaryKeyValuesInDb
    }
    
    
    uploadChunk <- function(chunk, pos) {
      ParallelLogger::logInfo("- Uploading rows ", pos, " through ", pos + nrow(chunk) - 1)
      checkColumnNames(table = chunk,
                       tableName = env$tableName,
                       zipFileName = zipFileName,
                       specifications = specifications)
      chunk <- checkAndFixDataTypes(table = chunk,
                                    tableName = env$tableName,
                                    zipFileName = zipFileName,
                                    specifications = specifications)
      chunk <- checkAndFixDuplicateRows(table = chunk,
                                        tableName = env$tableName,
                                        zipFileName = zipFileName,
                                        specifications = specifications)
      
      # Primary key fields cannot be NULL, so for some tables convert NAs to empty:
      toEmpty <- specifications %>%
        filter(.data$tableName == env$tableName & .data$emptyIsNa == "No") %>%
        select(.data$fieldName) %>%
        pull()
      if (length(toEmpty) > 0) {
        chunk <- chunk %>%
          dplyr::mutate_at(toEmpty, naToEmpty)
      }
      
      # Check if inserting data would violate primary key constraints:
      if (!is.null(env$primaryKeyValuesInDb)) {
        primaryKeyValuesInChunk <- unique(chunk[env$primaryKey])
        duplicates <- inner_join(env$primaryKeyValuesInDb,
                                 primaryKeyValuesInChunk,
                                 by = env$primaryKey)
        if (nrow(duplicates) != 0) {
          if ("database_id" %in% env$primaryKey || forceOverWriteOfSpecifications) {
            ParallelLogger::logInfo("- Found ", nrow(duplicates), " rows in database with the same primary key ",
                                    "as the data to insert. Deleting from database before inserting.")
            deleteFromServer(connection = connection,
                             schema = env$schema,
                             tableName = env$tableName,
                             keyValues = duplicates)
            
          } else {
            ParallelLogger::logInfo("- Found ", nrow(duplicates), " rows in database with the same primary key ",
                                    "as the data to insert. Removing from data to insert.")
            chunk <- chunk %>%
              anti_join(duplicates, by = env$primaryKey)
          }
          # Remove duplicates we already dealt with:
          env$primaryKeyValuesInDb <- env$primaryKeyValuesInDb %>%
            anti_join(duplicates, by = env$primaryKey)
        }
      }
      if (nrow(chunk) == 0) {
        ParallelLogger::logInfo("- No data left to insert")
      } else {
        insertDataIntoDb(connection = connection,
                         connectionDetails = connectionDetails,
                         schema = schema,
                         tableName = tableName,
                         data = chunk)
      }
    }
    if (file == "likelihood_profile.csv") {
      chunkSize <- 1e5
    } else {
      chunkSize <- 1e7
    }
    readr::read_csv_chunked(file = file.path(unzipFolder, file),
                            callback = uploadChunk,
                            chunk_size = chunkSize,
                            col_types = readr::cols(),
                            guess_max = 1e6,
                            progress = FALSE)
    
    # chunk <- readr::read_csv(file = file.path(unzipFolder, file),
    #                          col_types = readr::cols(),
    #                          guess_max = 1e6)
    # pos <- 1
    
  }
  files <- list.files(unzipFolder, "*.csv")
  invisible(lapply(files, uploadTable))
  delta <- Sys.time() - start
  writeLines(paste("Uploading data took", signif(delta, 3), attr(delta, "units")))
}

deleteFromServer <- function(connection, schema, tableName, keyValues) {
  asCharacter <- function(x) {
    if (is(x, "Date")) {
      return(format(x, "%Y-%m-%d"))
    } else {
      return(as.character(x))
    }
  }
  
  createSqlStatement <- function(i) {
    sql <- paste0("DELETE FROM ",
                  schema,
                  ".",
                  tableName,
                  "\nWHERE ",
                  paste(paste0(colnames(keyValues), " = '", sapply(keyValues[i, ], asCharacter), "'"), collapse = " AND "),
                  ";")
    return(sql)
  }
  batchSize <- 1000
  for (start in seq(1, nrow(keyValues), by = batchSize)) {
    end <- min(start + batchSize - 1, nrow(keyValues))
    sql <- sapply(start:end, createSqlStatement)
    sql <- paste(sql, collapse = "\n")
    DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE, runAsBatch = TRUE)
  }
}

deleteAllRecordsForDatabaseId <- function(connection,
                                          schema,
                                          tableName,
                                          databaseId) {
  sql <- "SELECT COUNT(*) FROM @schema.@table_name WHERE database_id = '@database_id';"
  sql <- SqlRender::render(sql = sql,
                           schema = schema,
                           table_name = tableName,
                           database_id = databaseId)
  databaseIdCount <- DatabaseConnector::querySql(connection, sql)[, 1]
  if (databaseIdCount != 0) {
    ParallelLogger::logInfo(sprintf("- Found %s rows in database with database ID '%s'. Deleting all before inserting.",
                                    databaseIdCount,
                                    databaseId))
    sql <- "DELETE FROM @schema.@table_name WHERE database_id = '@database_id';"
    sql <- SqlRender::render(sql = sql,
                             schema = schema,
                             table_name = tableName,
                             database_id = databaseId)
    DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }
}

insertDataIntoDb <- function(connection,
                             connectionDetails,
                             schema,
                             tableName,
                             data) {
  
  ParallelLogger::logInfo("- Inserting ", nrow(data), " rows into database")
  bulkLoad <- nrow(data) > 1e4 && !is.null(Sys.getenv("POSTGRES_PATH"))
  if (bulkLoad) {
    DatabaseConnector::executeSql(connection, "COMMIT;", progressBar = FALSE, reportOverallTime = FALSE)
    oldSciPen <- getOption("scipen")
    on.exit(options("scipen" = oldSciPen))
    options("scipen" = 6)
  }
  
  DatabaseConnector::insertTable(connection = connection,
                                 tableName = paste(schema, tableName, sep = "."),
                                 data = as.data.frame(data),
                                 dropTableIfExists = FALSE,
                                 createTable = FALSE,
                                 tempTable = FALSE,
                                 progressBar = TRUE,
                                 bulkLoad = bulkLoad)
}
