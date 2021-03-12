# Conditional Poisson regression (SCCS and SCRI methods) -------------------------------------------
# Simulation parameters
parameters <- data.frame(n = 10000, # Number of subjects
                         exposureRate = 0.01, 
                         backgroundOutcomeRate = 0.0001,
                         tar = 10, # Time at risk for each exposure
                         rr = 1) # Relative risk

simulate <- function(seed, parameters, method = "llr-binomial", conditionalPoisson = TRUE) {
  set.seed(seed)
  isSignal <- function(t, method, conditionalPoisson) {
    exposedTime <- t - tExposure 
    exposedTime[t <  tExposure] <- 0
    exposedTime[exposedTime > parameters$tar] <- parameters$tar
    unexposedTime <- t - exposedTime
    exposedOutcome <- tOutcome < t & tOutcome > tExposure & tOutcome < tExposure + parameters$tar
    unexposedOutcome <- tOutcome < t & !exposedOutcome
    data <- data.frame(time = c(exposedTime, unexposedTime),
                       outcome = c(exposedOutcome, unexposedOutcome),
                       exposure = c(rep(1, parameters$n,), rep(0, parameters$n)),
                       stratumId = rep(1:parameters$n, 2))
    data <- data[data$time > 0, ]
    
    # Verification: compute crude IRR:
    # (sum(data$outcome[data$exposure == 1]) / sum(data$time[data$exposure == 1])) / (sum(data$outcome[data$exposure == 0]) / sum(data$time[data$exposure == 0]))
    
    if (conditionalPoisson) {
      cyclopsData <- Cyclops::createCyclopsData(outcome ~ exposure + strata(stratumId) + offset(log(time)), modelType = "cpr", data = data)
    } else {
      cyclopsData <- Cyclops::createCyclopsData(outcome ~ exposure + offset(log(time)), modelType = "pr", data = data)
    }
    fit <- Cyclops::fitCyclopsModel(cyclopsData)
    # exp(coef(fit))
    
    if (method == "ci") {    
      ci <- confint(fit, "exposure", level = .90)
      return(ci[2] > 0)
    } else {
      if (coef(fit)["exposure"] > 0) {
        llNull <- Cyclops::getCyclopsProfileLogLikelihood(object = fit,
                                                          parm = "exposure",
                                                          x = 0,
                                                          includePenalty = FALSE)$value
        
        llr <- fit$log_likelihood - llNull
      } else {
        llr <- 0
      }
      
      if (method == "llr-binomial") {
        totalEvents <- sum(data$outcome)
        cv <- Sequential::CV.Binomial(N = totalEvents,
                                      alpha = 0.05,
                                      M = 1,
                                      z = sum(unexposedTime) / sum(exposedTime),
                                      GroupSizes = c(totalEvents))$cv
        return(llr > cv)
      } else if (method == "llr-poisson") {
        expectedEvents <- sum(data$time[data$exposure == 1]) * (sum(data$outcome[data$exposure == 0]) / sum(data$time[data$exposure == 0]))
        cv <- Sequential::CV.Poisson(SampleSize = expectedEvents,
                                     alpha = 0.05,
                                     M = 1,
                                     GroupSizes = c(expectedEvents))
        
        return(llr > cv)
      } else if (method == "llr-chisq") {
        return(1 - pchisq(2 * llr, df = 1) < 0.1)
      }
    }
    
  }
  
  tExposure = rexp(parameters$n,  parameters$exposureRate)
  tOutcome <- rexp(parameters$n,  parameters$backgroundOutcomeRate)
  
  # Compress time during TAR to achieve target RR:
  idxAfterTar <- tOutcome > tExposure + (parameters$tar * parameters$rr)
  idxDuringTar <- tOutcome > tExposure & tOutcome <= tExposure + (parameters$tar * parameters$rr)
  tOutcome[idxAfterTar] <- tOutcome[idxAfterTar] - ((1 - parameters$rr) * parameters$tar)
  tOutcome[idxDuringTar] <- tExposure[idxDuringTar] + ((tOutcome[idxDuringTar] - tExposure[idxDuringTar]) / parameters$rr)
  return(isSignal(t = 100, method = method, conditionalPoisson = conditionalPoisson))
}

# Compute type I error (probability of a signal when the null is true). Should be 0.05:
cluster <- ParallelLogger::makeCluster(10)
ParallelLogger::clusterRequire(cluster, "survival")

# mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-poisson", conditionalPoisson = TRUE)), na.rm = TRUE)
# 
# mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-poisson", conditionalPoisson = FALSE)), na.rm = TRUE)

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-binomial", conditionalPoisson = TRUE)), na.rm = TRUE)
# [1] 0.054

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-binomial", conditionalPoisson = FALSE)), na.rm = TRUE)

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "ci", conditionalPoisson = TRUE)), na.rm = TRUE)
# [1] 0.04704705

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "ci", conditionalPoisson = FALSE)), na.rm = TRUE)
# [1] 0.04004004

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-chisq", conditionalPoisson = TRUE)), na.rm = TRUE)
# [1] 0.047

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-chisq", conditionalPoisson = FALSE)), na.rm = TRUE)
# [1] 0.04

ParallelLogger::stopCluster(cluster)
# [1] 0.032


# Cox proportional hazards regression (cohort method) -----------------------------------------------------
parameters <- data.frame(n = 100000, # Number of subjects
                         pExposure = 0.5, # Probability of being in target cohort
                         backgroundHazard = 0.0001,
                         tar = 10, # Time at risk for each exposure
                         rr = 1, # Relative risk (hazard ratio)
                         maxT = 100) 

simulate <- function(seed, parameters, method = "llr-binomial") {
  set.seed(seed)
  
  llr <- function(observed, expected) {
    if (observed <= expected) {
      return(0)
    } else {
      return((expected - observed) + observed * log(observed / expected))
    }
  }
  
  computeCv <- function(data) {
    tempFolder <- tempfile()
    dir.create(tempFolder)
    on.exit(unlink(tempFolder, recursive = TRUE))
    
    timeExposed <- sum(data$time[data$exposure == 1])
    timeUnexposed <- sum(data$time[data$exposure == 0])
    outcomesExposed <- sum(data$outcome[data$exposure == 1])
    outcomesUnexposed <- sum(data$outcome[data$exposure == 0])
    Sequential::AnalyzeSetUp.CondPoisson(name = "TestA",
                                         SampleSizeType = "Events",
                                         K = outcomesExposed,
                                         cc = outcomesUnexposed,
                                         alpha = 0.05,
                                         M = 1,
                                         AlphaSpendType = "power-type",
                                         rho = 0.5,
                                         title = "n",
                                         address = tempFolder)
    x <- Sequential::Analyze.CondPoisson(name = "TestA",
                                         test = 1,
                                         events = outcomesExposed,
                                         PersonTimeRatio = timeExposed/timeUnexposed)
    return(as.numeric(x$CV))
  }
  
  isSignal <- function(t, method) {
    # Truncate at time of look:
    truncatedTime <- time
    idxTruncated <- tIndex + time > t
    truncatedTime[idxTruncated] <- t - tIndex[idxTruncated]
    truncatedOutcome <- outcome
    truncatedOutcome[idxTruncated] <- 0
    
    data <- data.frame(time = truncatedTime,
                       outcome = truncatedOutcome,
                       exposure = exposure)
    data <- data[data$time > 0, ]
    
    cyclopsData <- Cyclops::createCyclopsData(Surv(time, outcome) ~ exposure , modelType = "cox", data = data)
    fit <- Cyclops::fitCyclopsModel(cyclopsData, control = Cyclops::createControl(seed = seed))
    # exp(coef(fit))
    
    if (method == "ci") {    
      ci <- confint(fit, "exposureTRUE", level = .90)
      return(ci[2] > 0)
    } else {
      if (coef(fit)["exposureTRUE"] > 0) {
        llNull <- Cyclops::getCyclopsProfileLogLikelihood(object = fit,
                                                          parm = "exposureTRUE",
                                                          x = 0,
                                                          includePenalty = FALSE)$value
        
        llr <- fit$log_likelihood - llNull
      } else {
        llr <- 0
      }
      
      if (method == "llr-binomial") {
        totalEvents <- sum(data$outcome)
        cv <- Sequential::CV.Binomial(N = totalEvents,
                                      M = 1,
                                      z = sum(data$time[!data$exposure]) / sum(data$time[data$exposure]),
                                      GroupSizes = totalEvents)$cv
        return(llr > cv)
      } else if (method == "llr-poisson") {
        cv <- computeCv(data)
        return(llr > cv)
      } else if (method == "llr-chisq") {
        return(1 - pchisq(2 * llr, df = 1) < 0.1)
      }
    }
  }
  
  tIndex <- runif(parameters$n,  0, parameters$maxT)
  exposure <- runif(parameters$n) < parameters$pExposure
  tOutcome <- rexp(parameters$n,  parameters$backgroundHazard * (1 + ((parameters$rr - 1) * exposure)))
  outcome <- tOutcome < parameters$tar
  time <- rep(parameters$tar, parameters$n)
  time[outcome] <- tOutcome[outcome]
  return(isSignal(t = 100, method = method))
}

# Compute type I error (probability of a signal when the null is true). Should be 0.05:
cluster <- ParallelLogger::makeCluster(10)
ParallelLogger::clusterRequire(cluster, "survival")

# mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-poisson")), na.rm = TRUE)

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-binomial")), na.rm = TRUE)
# [1] 0.055

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "ci")), na.rm = TRUE)
# [1] 0.059

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-chisq")), na.rm = TRUE)
# [1] 0.059

ParallelLogger::stopCluster(cluster)



# Abstract Poisson ----------------------------------------------------------------------
parameters <- data.frame(n = 1000000, # Subjects
                         rate = 0.0001)

simulate <- function(seed, parameters, cv) {
  set.seed(seed)
  
  # From https://doi.org/10.1080/07474946.2011.539924, page 65:
  llr <- function(observed, expected) {
    if (observed <= expected) {
      return(0)
    } else {
      return((expected - observed) + observed * log(observed / expected))
    }
  }
  
  observed <- sum(rpois(parameters$n, parameters$rate))
  expected <- parameters$n * parameters$rate
  llr <- llr(observed = observed, expected = expected)
  return(llr > cv)
}

# Compute type I error (probability of a signal when the null is true). Should be 0.05:
expected <- parameters$n * parameters$rate
cv <- Sequential::CV.Poisson(SampleSize = expected,
                             alpha = 0.05,
                             M = 1,
                             GroupSizes = c(expected))

cluster <- ParallelLogger::makeCluster(10)
mean(unlist(ParallelLogger::clusterApply(cluster, 1:10000, simulate, parameters = parameters, cv = cv)), na.rm = TRUE)
# [1] 0.04210842

ParallelLogger::stopCluster(cluster)


# Abstract Poisson with finite comparator --------------------------------------------
parameters <- data.frame(n = 100000, # Subjects exposed,
                         nHistoric = 100000,
                         rate = 0.0002)

simulate <- function(seed, parameters, method = "llr-chisq") {
  set.seed(seed)
  observed <- sum(rpois(parameters$n, parameters$rate))
  observedHistoric <- sum(rpois(parameters$nHistoric, parameters$rate))
  data <- data.frame(outcome = c(observed, observedHistoric),
                     exposure = c(1, 0),
                     time = c(parameters$n, parameters$nHistoric))
  
  cyclopsData <- Cyclops::createCyclopsData(outcome ~ exposure + offset(log(time)), modelType = "pr", data = data)
  fit <- Cyclops::fitCyclopsModel(cyclopsData)
  llNull <- Cyclops::getCyclopsProfileLogLikelihood(object = fit,
                                                    parm = "exposure",
                                                    x = 0,
                                                    includePenalty = FALSE)$value
  if (method == "llr-chisq") {
    lrt <- 2*(fit$log_likelihood - llNull)
    return(pchisq(lrt, df = 1, lower.tail = FALSE) < 0.05)
  } else if (method == "llr-poisson") {
    # Warning: does not run in multi-threading
    tempFolder <- tempfile()
    dir.create(tempFolder)
    on.exit(unlink(tempFolder, recursive = TRUE))
    
    Sequential::AnalyzeSetUp.CondPoisson(name = "TestA",
                                         SampleSizeType = "Events",
                                         K = observed,
                                         cc = observedHistoric,
                                         alpha = 0.05,
                                         M = 1,
                                         AlphaSpendType = "power-type",
                                         rho = 0.5,
                                         title = "n",
                                         address = tempFolder)
    cv <- Sequential::Analyze.CondPoisson(name = "TestA",
                                          test = 1,
                                          events = observed,
                                          PersonTimeRatio = parameters$n / parameters$nHistoric)$CV
    llr <- fit$log_likelihood - llNull
    if (coef(fit)["exposure"] > 0) {
      llr <- fit$log_likelihood - llNull
    } else {
      llr <- 0
    }
    return(llr > cv)
  } else if (method == "llr-binomial") {
    # Warning: does not run in multi-threading
    # tempFolder <- tempfile()
    # dir.create(tempFolder)
    # on.exit(unlink(tempFolder, recursive = TRUE))
    # Sequential::AnalyzeSetUp.Binomial(name = "TestA",
    #                                   N = observed + observedHistoric,
    #                                   zp = parameters$nHistoric / parameters$n,
    #                                   M = 1,
    #                                   AlphaSpendType="Wald",
    #                                   rho = 0.5,
    #                                   title = "n",
    #                                   address = tempFolder)
    # cv <- as.numeric(Sequential::Analyze.Binomial(name = "TestA",
    #                                               test = 1,
    #                                               z = parameters$nHistoric / parameters$n,
    #                                               cases = observed,
    #                                               controls = observedHistoric)$CV)
    # return(observed >= cv)
    
    cv <- Sequential::CV.Binomial(N = observed + observedHistoric,
                                  M = 1,
                                  z = parameters$nHistoric / parameters$n,
                                  GroupSizes = observed + observedHistoric)$cv
    
    if (coef(fit)["exposure"] > 0) {
      llr <- fit$log_likelihood - llNull
    } else {
      llr <- 0
    }
    # exp(coef(fit))
    # unlink(tempFolder, recursive = TRUE)
    return(llr > cv)
  }
}

# Compute type I error (probability of a signal when the null is true). Should be 0.05:

cluster <- ParallelLogger::makeCluster(1)
mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-binomial")), na.rm = TRUE)
# [1] 0.032

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-poisson")), na.rm = TRUE)
# [1] 0.039

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-chisq")), na.rm = TRUE)
# [1] 0.054

ParallelLogger::stopCluster(cluster)
