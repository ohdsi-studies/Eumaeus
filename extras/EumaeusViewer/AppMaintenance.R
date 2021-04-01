# Copy to ShinyDeploy:
targetFolder <- "C:/Users/mschuemi/git/ShinyDeploy/Eumaeus"
if (!file.exists(targetFolder)) {
  dir.create(targetFolder)
}
sourceFolder <- setwd(targetFolder)
system("git pull")
setwd(sourceFolder)

file.copy(from = ".", to = targetFolder, overwrite = TRUE, recursive = TRUE)
unlink(file.path(targetFolder, "AppMaintenance.R"))



rstudioapi::openProject(file.path(targetFolder), newSession = TRUE)
