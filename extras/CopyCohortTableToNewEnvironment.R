# This code can be used to copy the cohort table to a new database environment (holding exactly the same CDM data)
tempCohortFile <- file.path(outputFolder, "cohortTable.zip")

# Fetch entire cohort table ------------------------------------
connection <- connect(connectionDetails)
cohortAndromeda <- Andromeda::andromeda()
table <- renderTranslateQuerySqlToAndromeda(connection = connection, 
                                            andromeda = cohortAndromeda,
                                            andromedaTableName = "cohort",
                                            sql = "SELECT * FROM @cohort_database_schema.@cohort_table;",
                                            cohort_database_schema = cohortDatabaseSchema,
                                            cohort_table = cohortTable)
# For quick check: count number of rows in cohort and PERSON table:
renderTranslateQuerySql(connection = connection, 
                        sql = "SELECT COUNT(*) FROM @cohort_database_schema.@cohort_table;", 
                        cohort_database_schema = cohortDatabaseSchema,
                        cohort_table = cohortTable)

renderTranslateQuerySql(connection = connection, 
                        sql = "SELECT COUNT(*) FROM @cdm_database_schema.person;", 
                        cdm_database_schema = cdmDatabaseSchema)

disconnect(connection)
Andromeda::saveAndromeda(cohortAndromeda, tempCohortFile)

# Upload to new environment. Change credentials first!
cohortAndromeda <- Andromeda::loadAndromeda(tempCohortFile)

connection <- connect(connectionDetails)
env <- new.env()
env$first <- TRUE
uploadChunk <- function(chunk, env) {
  insertTable(connection = connection,
              databaseSchema = cohortDatabaseSchema,
              tableName = cohortTable,
              data = chunk,
              dropTableIfExists = FALSE,
              createTable = env$first,
              tempTable = FALSE,
              bulkLoad = TRUE)
  env$first <- FALSE
}
Andromeda::batchApply(cohortAndromeda$cohort, uploadChunk, env = env, batchSize = 1e6, progressBar = TRUE)

# For quick check: count number of rows in cohort and PERSON table:
renderTranslateQuerySql(connection = connection, 
                        sql = "SELECT COUNT(*) FROM @cohort_database_schema.@cohort_table;", 
                        cohort_database_schema = cohortDatabaseSchema,
                        cohort_table = cohortTable)

renderTranslateQuerySql(connection = connection, 
                        sql = "SELECT COUNT(*) FROM @cdm_database_schema.person;", 
                        cdm_database_schema = cdmDatabaseSchema)

disconnect(connection)
Andromeda::close(cohortAndromeda)

# Drop the temp cohort file
unlink(tempCohortFile)
