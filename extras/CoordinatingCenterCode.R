# This file contains code to be used by the study coordinator to download the files from the SFTP server, and to upload them to the results database.
library(Eumaeus)
library(OhdsiSharing)

allDbsFolder <- "r:/Eumaeus/AllDbs"
# dir.create(allDbsFolder)

# Download files from SFTP server -----------------------------------------------------------------
connection <- sftpConnect(privateKeyFileName = "c:/home/keyfiles/study-coordinator-eumaeus",
                          userName = "study-coordinator-eumaeus")

# sftpMkdir(connection, "eumaeus")

sftpCd(connection, "eumaeus")
files <- sftpLs(connection)
files

sftpGetFiles(connection, files$fileName, localFolder = allDbsFolder)

# DANGER!!! Remove files from server:
sftpRm(connection, files$fileName)

sftpDisconnect(connection)


# Upload results to database -----------------------------------------------------------------------
library(DatabaseConnector)
connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = paste(keyring::key_get("ohdsiPostgresServer"),
                                                            keyring::key_get("ohdsiPostgresShinyDatabase"),
                                                            sep = "/"),
                                             user = keyring::key_get("ohdsiPostgresUser"),
                                             password = keyring::key_get("ohdsiPostgresPassword"))
schema <- "eumaeus"

# Do this only once!
# createResultsDataModel(connectionDetails, schema)

# # After the tables have been created:
# sql <- "grant select on all tables in schema eumaeus to rw_grp;"
# sql <- "grant select on all tables in schema eumaeus to eumaeus_readonly;"
#
# # Next time, before creating tables:
# sql <- "grant usage on schema eumaeus to group rw_grp;
# alter default privileges in schema eumaeus grant select on tables to group rw_grp;
# alter default privileges in schema eumaeus grant all on tables to group rw_grp;"
# connection <- connect(connectionDetails)
# executeSql(connection, sql)
# disconnect(connection)
# 
# Upload data

uploadedFolder <- file.path(allDbsFolder, "uploaded")

zipFilesToUpload <- list.files(path = allDbsFolder,
                               pattern = ".zip",
                               recursive = FALSE)

for (i in (1:length(zipFilesToUpload))) {
  uploadResultsToDatabase(connectionDetails = connectionDetails,
                          schema = schema,
                          zipFileName = file.path(allDbsFolder, zipFilesToUpload[i]),
                          purgeSiteDataBeforeUploading = FALSE)
  # Move to uploaded folder:
  file.rename(file.path(allDbsFolder, zipFilesToUpload[i]), file.path(uploadedFolder, zipFilesToUpload[i]))
}
