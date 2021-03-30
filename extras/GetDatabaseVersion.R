getDatabaseVersion <- function(connectionDetails, cdmDatabaseSchema) {
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  if ("_VERSION" %in% DatabaseConnector::getTableNames(connection, cdmDatabaseSchema)) {
    version <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection, 
      sql = "SELECT * FROM @cdm_database_schema._version;",
      cdm_database_schema = cdmDatabaseSchema
    )
  } else {
    version <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection, 
      sql = "SELECT * FROM @cdm_database_schema.cdm_source;",
      cdm_database_schema = cdmDatabaseSchema
    )
  }  
  print(version)
}