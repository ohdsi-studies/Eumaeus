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

library(Eumaeus)

options(andromedaTempFolder = "s:/andromedaTemp")
options(sqlRenderTempEmulationSchema = NULL)

maxCores <- parallel::detectCores() - 1

# For bulk uploading synthetic outcomes:
Sys.setenv("AWS_OBJECT_KEY" = "bulk")
Sys.setenv("AWS_ACCESS_KEY_ID" = keyring::key_get("bulkUploadS3Key"))
Sys.setenv("AWS_SECRET_ACCESS_KEY" = keyring::key_get("bulkUploadS3Secret"))
Sys.setenv("AWS_BUCKET_NAME" = keyring::key_get("bulkUploadS3Bucket"))
Sys.setenv("AWS_DEFAULT_REGION" = "us-east-1")
Sys.setenv("AWS_SSE_TYPE" = "AES256")
Sys.setenv("DATABASE_CONNECTOR_BULK_UPLOAD" = TRUE)

# Details specific to MDCD:
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
                                                                connectionString = keyring::key_get("redShiftConnectionStringOhdaMdcd"),
                                                                user = keyring::key_get("redShiftUserName"),
                                                                password = keyring::key_get("redShiftPassword"))
outputFolder <- "r:/Eumaeus/mdcd"
cdmDatabaseSchema <- "cdm_truven_mdcd_v1476"
cohortDatabaseSchema <- "scratch_mschuemi"
cohortTable <- "eumaeus_mdcd"
databaseId <- "IBM_MDCD"
databaseName <- "IBM Health MarketScan® Multi-State Medicaid Database"
databaseDescription <- "IBM MarketScan® Multi-State Medicaid Database (MDCD) adjudicated US health insurance claims for Medicaid enrollees from multiple states and includes hospital discharge diagnoses, outpatient diagnoses and procedures, and outpatient pharmacy claims as well as ethnicity and Medicare eligibility. Members maintain their same identifier even if they leave the system for a brief period however the dataset lacks lab data."


# # Details specific to MDCR:
# connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
#                                                                 connectionString = keyring::key_get("redShiftConnectionStringOhdaMdcr"),
#                                                                 user = keyring::key_get("redShiftUserName"),
#                                                                 password = keyring::key_get("redShiftPassword"))
# outputFolder <- "s:/Eumaeus/mdcr"
# cdmDatabaseSchema <- "cdm_truven_mdcr_v1477"
# cohortDatabaseSchema <- "scratch_mschuemi"
# cohortTable <- "eumaeus_mdcr"
# databaseId <- "IBM_MDCR"
# databaseName <- "IBM MarketScan® Medicare Supplemental and Coordination of Benefits Database"
# databaseDescription <- "IBM MarketScan® Medicare Supplemental and Coordination of Benefits Database (MDCR) represents health services of retirees in the United States with primary or Medicare supplemental coverage through privately insured fee-for-service, point-of-service, or capitated health plans.  These data include adjudicated health insurance claims (e.g. inpatient, outpatient, and outpatient pharmacy). Additionally, it captures laboratory tests for a subset of the covered lives."
# 
# 
# 
# # Details specific to CCAE:
# connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
#                                                                 connectionString = keyring::key_get("redShiftConnectionStringCcae"),
#                                                                 user = keyring::key_get("redShiftUserName"),
#                                                                 password = keyring::key_get("redShiftPassword"))
# outputFolder <- "d:/Eumaeus/CCAE"
# cdmDatabaseSchema <- "cdm"
# cohortDatabaseSchema <- "scratch_mschuemi5"
# cohortTable <- "mschuemi_vac_surv_ccae"
# databaseId <- "CCAE"
# databaseName <- "IBM MarketScan Commercial Claims and Encounters Database"
# databaseDescription <- "IBM MarketScan® Commercial Claims and Encounters Database (CCAE) represent data from individuals enrolled in United States employer-sponsored insurance health plans. The data includes adjudicated health insurance claims (e.g. inpatient, outpatient, and outpatient pharmacy) as well as enrollment data from large employers and health plans who provide private healthcare coverage to employees, their spouses, and dependents. Additionally, it captures laboratory tests for a subset of the covered lives. This administrative claims database includes a variety of fee-for-service, preferred provider organizations, and capitated health plans."
# 
# 
# # Details specific to OptumEhr:
# connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
#                                                                 connectionString = keyring::key_get("redShiftConnectionStringOptumEhr"),
#                                                                 user = keyring::key_get("redShiftUserName"),
#                                                                 password = keyring::key_get("redShiftPassword"))
# outputFolder <- "d:/Eumaeus/OptumEhr"
# cdmDatabaseSchema <- "cdm"
# cohortDatabaseSchema <- "scratch_mschuemi"
# cohortTable <- "mschuemi_vac_surv_optum_ehr"
# databaseId <- "OptumEhr"
# databaseName <- "Optum de-identified Electronic Health Record Dataset"
# databaseDescription <- "Optum© de-identified Electronic Health Record Dataset is derived from dozens of healthcare provider organizations in the United States (that include more than 700 hospitals and 7,000 Clinics treating more than 103 million patients) receiving care in the United States. The medical record data includes clinical information, inclusive of prescriptions as prescribed and administered, lab results, vital signs, body measurements, diagnoses, procedures, and information derived from clinical Notes using Natural Language Processing (NLP)."
# 
# # Details specific to CPRD:
# connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
#                                                                 connectionString = keyring::key_get("redShiftConnectionStringCprd"),
#                                                                 user = keyring::key_get("redShiftUserName"),
#                                                                 password = keyring::key_get("redShiftPassword"))
# outputFolder <- "r:/Eumaeus/Cprd"
# cdmDatabaseSchema <- "cdm"
# cohortDatabaseSchema <- "scratch_mschuemi_2"
# cohortTable <- "mschuemi_vac_surv_cprd"
# databaseId <- "Cprd"
# databaseName <- "Clinical Practice Research Datalink (CPRD)"
# databaseDescription <- "The Clinical Practice Research Datalink (CPRD) is a governmental, not-for-profit research service, jointly funded by the NHS National Institute for Health Research (NIHR) and the Medicines and Healthcare products Regulatory Agency (MHRA), a part of the Department of Health, United Kingdom (UK).  CPRD consists of data collected from UK primary care for all ages.  This includes conditions, observations, measurements, and procedures that the general practitioner is made aware of in additional to any prescriptions as prescribed by the general practitioner.  In addition to primary care, there are also linked secondary care records for a small number of people."
# 
# # Details specific to Optum DoD:
# connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
#                                                                 connectionString = keyring::key_get("redShiftConnectionStringOptumDod"),
#                                                                 user = keyring::key_get("redShiftUserName"),
#                                                                 password = keyring::key_get("redShiftPassword"))
# outputFolder <- "r:/Eumaeus/OptumDod"
# cdmDatabaseSchema <- "cdm"
# cohortDatabaseSchema <- "scratch_mschuemi6"
# cohortTable <- "mschuemi_vac_surv_optum_dod"
# databaseId <- "OptumDod"
# databaseName <- "Optum Clinformatics Extended Data Mart - Date of Death (DOD)"
# databaseDescription <- "Optum Clinformatics Extended DataMart is an adjudicated US administrative health claims database for members of private health insurance, who are fully insured in commercial plans or in administrative services only (ASOs), Legacy Medicare Choice Lives (prior to January 2006), and Medicare Advantage (Medicare Advantage Prescription Drug coverage starting January 2006).  The population is primarily representative of commercial claims patients (0-65 years old) with some Medicare (65+ years old) however ages are capped at 90 years.  It includes data captured from administrative claims processed from inpatient and outpatient medical services and prescriptions as dispensed, as well as results for outpatient lab tests processed by large national lab vendors who participate in data exchange with Optum.  This dataset also provides date of death (month and year only) for members with both medical and pharmacy coverage from the Social Security Death Master File (however after 2011 reporting frequency changed due to changes in reporting requirements) and location information for patients is at the US state level."



# 
# 
# runCohortDiagnostics(connectionDetails = connectionDetails,
#                      cdmDatabaseSchema = cdmDatabaseSchema,
#                      cohortDatabaseSchema = cohortDatabaseSchema,
#                      cohortTable = cohortTable,
#                      databaseId = databaseId,
#                      databaseName = databaseName,
#                      databaseDescription = databaseDescription,
#                      outputFolder = outputFolder,
#                      createCohorts = TRUE,
#                      runCohortDiagnostics = TRUE)
# 
# CohortDiagnostics::preMergeDiagnosticsFiles(file.path(outputFolder, "cohortDiagnostics"))
# CohortDiagnostics::launchDiagnosticsExplorer(file.path(outputFolder, "cohortDiagnostics"))
# 
execute(connectionDetails = connectionDetails,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        databaseId = databaseId,
        databaseName = databaseName,
        databaseDescription = databaseDescription,
        outputFolder = outputFolder,
        maxCores = maxCores,
        exposureIds = getExposuresOfInterest()$exposureId,
        verifyDependencies = TRUE,
        createCohorts = F,
        synthesizePositiveControls = F,
        runCohortMethod = F,
        runSccs = F,
        runCaseControl = F,
        runHistoricalComparator = F,
        generateDiagnostics = F,
        computeCriticalValues = TRUE,
        createDbCharacterization = F,
        exportResults = F)

uploadResults(outputFolder = outputFolder,
              privateKeyFileName = "c:/home/keyfiles/study-data-site-covid19.dat",
              userName = "study-data-site-covid19")

# JnJ specific code to store database version:
source("extras/GetDatabaseVersion.R")
version <- getDatabaseVersion(connectionDetails, cdmDatabaseSchema)
readr::write_csv(version, file.path(outputFolder, "version.csv"))
