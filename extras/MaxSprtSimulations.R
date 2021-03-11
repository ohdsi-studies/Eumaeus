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
    } else if (method == "llr-binomial") {
      llNull <- Cyclops::getCyclopsProfileLogLikelihood(object = fit,
                                                        parm = "exposure",
                                                        x = 0,
                                                        includePenalty = FALSE)$value
      llr <- fit$log_likelihood - llNull
      totalEvents <- sum(data$outcome)
      cv <- Sequential::CV.Binomial(N = totalEvents,
                                    alpha = 0.05,
                                    M = 1,
                                    z = sum(unexposedTime) / sum(exposedTime),
                                    GroupSizes = c(totalEvents))$cv
      return(llr > cv)
    } else if (method == "llr-poisson") {
      llNull <- Cyclops::getCyclopsProfileLogLikelihood(object = fit,
                                                        parm = "exposure",
                                                        x = 0,
                                                        includePenalty = FALSE)$value
      llr <- fit$log_likelihood - llNull
      expectedEvents <- sum(data$time[data$exposure == 1]) * (sum(data$outcome[data$exposure == 0]) / sum(data$time[data$exposure == 0]))
      cv <- Sequential::CV.Poisson(SampleSize = expectedEvents,
                                   alpha = 0.05,
                                   M = 1,
                                   GroupSizes = c(expectedEvents))
      
      return(llr > cv)
    } else if (method == "llr-chisq") {
      llNull <- Cyclops::getCyclopsProfileLogLikelihood(object = fit,
                                                        parm = "exposure",
                                                        x = 0,
                                                        includePenalty = FALSE)$value
      lrt <- 2*(fit$log_likelihood - llNull)
      return(pchisq(lrt, df = 1, lower.tail = FALSE) < 0.05)
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

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-poisson", conditionalPoisson = TRUE)), na.rm = TRUE)
# [1] 0.121

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-poisson", conditionalPoisson = FALSE)), na.rm = TRUE)
# [1] 0.102

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-binomial", conditionalPoisson = TRUE)), na.rm = TRUE)
# [1] 0.119

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-binomial", conditionalPoisson = FALSE)), na.rm = TRUE)
# [1] 0.102

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "ci", conditionalPoisson = TRUE)), na.rm = TRUE)
# [1] 0.04704705

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "ci", conditionalPoisson = FALSE)), na.rm = TRUE)
# [1] 0.04004004

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-chisq", conditionalPoisson = TRUE)), na.rm = TRUE)
# [1] 0.041

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-chisq", conditionalPoisson = FALSE)), na.rm = TRUE)
# [1] 0.044

ParallelLogger::stopCluster(cluster)



# Cox proportional hazards regression (cohort method) -----------------------------------------------------
parameters <- data.frame(n = 100000, # Number of subjects
                         pExposure = 0.5, # Probability of being in target cohort
                         backgroundHazard = 0.0001,
                         tar = 10, # Time at risk for each exposure
                         rr = 1, # Relative risk (hazard ratio)
                         maxT = 100) 

simulate <- function(seed, parameters, method = "llr-binomial", cyclops = TRUE) {
  set.seed(seed)
  
  llr <- function(observed, expected) {
    if (observed <= expected) {
      return(0)
    } else {
      return((expected - observed) + observed * log(observed / expected))
    }
  }
  
  isSignal <- function(t, method, cyclops) {
    # Truncate at time of look:
    truncatedTime <- time
    idxTruncated <- tIndex + time > t
    truncatedTime[idxTruncated] <- t - tIndex[idxTruncated]
    
    data <- data.frame(time = truncatedTime,
                       outcome = outcome,
                       exposure = exposure)
    data <- data[data$time > 0, ]
    
    if (cyclops) {
      cyclopsData <- Cyclops::createCyclopsData(Surv(time, outcome) ~ exposure , modelType = "cox", data = data)
      fit <- Cyclops::fitCyclopsModel(cyclopsData, control = Cyclops::createControl(seed = seed))
      # exp(coef(fit))
      
      if (method == "ci") {    
        ci <- confint(fit, "exposureTRUE", level = .90)
        return(ci[2] > 0)
      } else if (method == "llr-binomial") {
        llNull <- Cyclops::getCyclopsProfileLogLikelihood(object = fit,
                                                          parm = "exposureTRUE",
                                                          x = 0,
                                                          includePenalty = FALSE)$value
        llr <- fit$log_likelihood - llNull
        totalEvents <- sum(data$outcome)
        cv <- Sequential::CV.Binomial(N = totalEvents,
                                      alpha = 0.05,
                                      M = 1,
                                      p = mean(data$outcome[!data$exposure]),
                                      GroupSizes = c(totalEvents))$cv
        return(llr > cv)
      } else if (method == "llr-poisson") {
        llNull <- Cyclops::getCyclopsProfileLogLikelihood(object = fit,
                                                          parm = "exposureTRUE",
                                                          x = 0,
                                                          includePenalty = FALSE)$value
        llr <- fit$log_likelihood - llNull
        expectedEvents <- nrow(data) * mean(data$outcome[!data$exposure])
        cv <- Sequential::CV.Poisson(SampleSize = expectedEvents,
                                     alpha = 0.05,
                                     M = 1,
                                     GroupSizes = c(expectedEvents))
        # system.time(
        #   cv <- Sequential::CV.CondPoisson(Inference = "exact",
        #                                    StopType = "Tal",
        #                                    cc = sum(data$outcome),
        #                                    T = 1,
        #                                    alpha = 0.05,
        #                                    M = 1)
        # )
        # # Error in while (alpharef <= alphar1 & i <= (imax - 1)) { : 
        # #     missing value where TRUE/FALSE needed
        # #   Timing stopped at: 654.6 0.2 655.2
        # system.time(
        #   cv <- Sequential::CV.CondPoisson(Inference = "exact",
        #                                    StopType = "Cases",
        #                                    cc = sum(data$outcome),
        #                                    K = sum(data$outcome),
        #                                    alpha = 0.05,
        #                                    M = 1)
        # )
        # # Error in while (alpharef <= alphar1 & i <= (imax - 1)) { : 
        # #     missing value where TRUE/FALSE needed
        # #   Timing stopped at: 641.1 0.14 641.3
        
        return(llr > cv)
      } else if (method == "llr-chisq") {
        llNull <- Cyclops::getCyclopsProfileLogLikelihood(object = fit,
                                                          parm = "exposureTRUE",
                                                          x = 0,
                                                          includePenalty = FALSE)$value
        lrt <- 2*(fit$log_likelihood - llNull)
        return(pchisq(lrt, df = 1, lower.tail = FALSE) < 0.05)
      }
    } else {
      # cyclops == FALSE
      expectedEvents <- sum(data$exposure) * mean(data$outcome[!data$exposure])
      observedEvents <- sum(data$outcome[data$exposure])
      llr <- llr(observed = observedEvents, expected = expectedEvents)
      if (method == "llr-binomial") {
        cv <- Sequential::CV.Binomial(N = observedEvents,
                                      alpha = 0.05,
                                      M = 1,
                                      p = mean(data$outcome[!data$exposure]),
                                      GroupSizes = c(observedEvents))$cv
        return(llr > cv)
      } else if (method == "llr-poisson") {
        cv <- Sequential::CV.Poisson(SampleSize = expectedEvents,
                                     alpha = 0.05,
                                     M = 1,
                                     GroupSizes = c(expectedEvents))
        
        return(llr > cv)
      }
    }
  }
  
  tIndex <- runif(parameters$n,  0, parameters$maxT)
  exposure <- runif(parameters$n) < parameters$pExposure
  tOutcome <- rexp(parameters$n,  parameters$backgroundHazard * (1 + ((parameters$rr - 1) * exposure)))
  outcome <- tOutcome < parameters$tar
  time <- rep(parameters$tar, parameters$n)
  time[outcome] <- tOutcome[outcome]
  return(isSignal(t = 100, method = method, cyclops = cyclops))
}

# Compute type I error (probability of a signal when the null is true). Should be 0.05:
cluster <- ParallelLogger::makeCluster(10)
ParallelLogger::clusterRequire(cluster, "survival")

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-poisson", cyclops = TRUE)), na.rm = TRUE)
# [1] 0.1104418

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-poisson", cyclops = FALSE)), na.rm = TRUE)
# [1] 0.1192385

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-binomial", cyclops = TRUE)), na.rm = TRUE)
# [1] 0.09538153

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-binomial", cyclops = FALSE)), na.rm = TRUE)
# [1] 0.0813253

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "ci", cyclops = TRUE)), na.rm = TRUE)
# [1] 0.0562249

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-chisq", cyclops = TRUE)), na.rm = TRUE)
# [1] 0.05823293

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
