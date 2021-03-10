# Simulation parameters
parameters <- data.frame(n = 10000, # Number of subjects
                         exposureRate = 0.01, 
                         backgroundOutcomeRate = 0.0001,
                         tar = 10, # Time at risk for each exposure
                         rr = 1) # Relative risk

simulate <- function(dummy, parameters, method = "llr-binomial", conditionalPoisson = TRUE) {
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

# Compute type I error (probabiliy of a signal when the null is true). Should be 0.05:
cluster <- ParallelLogger::makeCluster(10)
ParallelLogger::clusterRequire(cluster, "survival")

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-poisson", conditionalPoisson = TRUE)), na.rm = TRUE)
# [1] 0.123

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-poisson", conditionalPoisson = FALSE)), na.rm = TRUE)
# [1] 0.119

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-binomial", conditionalPoisson = TRUE)), na.rm = TRUE)
# [1] 0.115

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-binomial", conditionalPoisson = FALSE)), na.rm = TRUE)
# [1] 0.104

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "ci", conditionalPoisson = TRUE)), na.rm = TRUE)
# [1] 0.04008016

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "ci", conditionalPoisson = FALSE)), na.rm = TRUE)
# [1] 0.048

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-chisq", conditionalPoisson = TRUE)), na.rm = TRUE)
# [1] 0.058

mean(unlist(ParallelLogger::clusterApply(cluster, 1:1000, simulate, parameters = parameters, method = "llr-chisq", conditionalPoisson = FALSE)), na.rm = TRUE)
# [1] 0.045

ParallelLogger::stopCluster(cluster)
