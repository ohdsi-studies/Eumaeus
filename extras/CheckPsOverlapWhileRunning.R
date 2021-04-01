psDiagFolder <- file.path(outputFolder, "psDiag")
if (!file.exists(psDiagFolder)) {
  dir.create(psDiagFolder)
}

tcFolders <- list.files(file.path(outputFolder, "cohortMethod"), pattern = "^t[0-9]+_c[0-9]+$", full.names = TRUE)
tcFolder = tcFolders[1]

findPs <- function(periodFolder) {
  return(list.files(periodFolder, pattern = "^Ps_l1_p1_t[0-9]+_c[0-9]+.rds$", full.names = TRUE))
}

findLatestPs <- function(tcFolder) {
  periodFolders <- list.files(tcFolder, pattern = "^cmOutput_t[1-9]+$", full.names = TRUE)  
  seqId <- as.numeric(gsub(".*cmOutput_t", "", periodFolders))
  periodFolders <- periodFolders[order(seqId)]
  psFiles <- unlist(sapply(periodFolders, findPs))
  return(psFiles[length(psFiles)])
}

psFiles <- unlist(sapply(tcFolders, findLatestPs))

psFile = psFiles[1]
plotPsOverlap <- function(psFile) {
  ps <- readRDS(psFile)
  fileName <- file.path(psDiagFolder, gsub("/cmOutput.*", ".png", gsub(".*cohortMethod/", "", psFile)))
  CohortMethod::plotPs(ps,
                       title = gsub("/Ps_.*$", "", gsub(".*cohortMethod/", "", psFile)),
                       fileName = fileName)
}
invisible(sapply(psFiles, plotPsOverlap))

# Exploring some issues ------------------------------
ps <- readRDS("r:/Eumaeus/mdcd/cohortMethod/t211851_c211852/cmOutput_t9/Ps_l1_p1_t211851_c211852.rds")
head(ps)
str(ps)
metaData <- attr(ps, "metaData")
metaData$psHighCorrelation
