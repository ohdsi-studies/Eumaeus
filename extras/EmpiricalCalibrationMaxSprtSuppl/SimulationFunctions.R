# Abstract Poisson (historic comparator method) -------------------------------------------------------------

#' Create parameters for the abstract Poisson simulation
#'
#' @param n                     Number of subjects to simulate.
#' @param baseLineRate          The baseline Poisson rate of the outcome.
#' @param trueEffectSizes       A vector of true effect sizes (IRRs) to simulate.
#' @param controlsPerEffectSize Number of control exposure-outcomes per true effect size.
#' @param looks                 Number of (equally spaced) looks at the data.
#' @param alpha                 The target type 1 error rate.
#' @param null                  A list containing the parameters of the true null distribution.
#'
#' @return
#' A list containing the specified parameters.
#'
#' @export
createAbstractPoissonSimulationParameters <- function(n = 100000,
                                                      baseLineRate = 0.0001,
                                                      trueEffectSizes = c(1, 1.5, 2, 4),
                                                      controlsPerEffectSize = 50,
                                                      looks = 10,
                                                      alpha = 0.05,
                                                      null = list(mu = 0.2, sigma = 0.2),
                                                      calibration = "cv") {
  parameters <- list(
    n = n,
    calibration = calibration,
    baseLineRate = baseLineRate,
    trueEffectSizes = trueEffectSizes,
    controlsPerEffectSize = controlsPerEffectSize,
    looks = looks,
    alpha = alpha,
    null = null
  )
  return(parameters)
}

#' Run a single abstract Poisson simulation
#'
#' @param parameters Parameters as created using the
#' \code{\link{createAbstractPoissonSimulationParameters}} function.
#' @iterations       Number of times to repeat the simulation.
#' @threads          Number of parallel threads to use.
#'
#' @return
#' A tibble with results of the simulation runs.
#'
#' @export
simulateAbstractPoisson <- function(parameters, iterations = 100, threads = parallel::detectCores()) {
  cluster <- ParallelLogger::makeCluster(threads)
  on.exit(ParallelLogger::stopCluster(cluster))
  ParallelLogger::clusterRequire(cluster, "dplyr")
  runs <- ParallelLogger::clusterApply(cluster, 1:iterations, simulateSingleAbstractPoisson, parameters = parameters)
  runs <- bind_rows(runs)
  return(runs)
}

simulateSingleAbstractPoisson <- function(seed, parameters) {
  set.seed(seed)
  
  # Compute the log likelihood at various grid points using an adaptive grid aimed at keeping possible
  # error (between linear interpolation and true LL) down while minimizing the number of grid points.
  computeAbstractPoissonLlProfile <- function(observed, expected, tolerance = 0.1, bounds = c(log(0.1), log(10))) {
    logRr <- log(observed / expected)
    
    if (!is.na(logRr) && logRr > bounds[1] && logRr < bounds[2]) {
      profile <- tibble(
        point = logRr,
        value = dpois(observed, observed, log = TRUE)
      )
    } else {
      profile <- tibble()
    }
    grid <- seq(bounds[1], bounds[2], length.out = 10)
    while (length(grid) != 0) {
      ll <- tibble(
        point = grid,
        value = dpois(observed, expected * exp(grid), log = TRUE)
      )
      
      profile <- bind_rows(profile, ll) %>%
        arrange(.data$point)
      deltaX <- profile$point[2:nrow(profile)] - profile$point[1:(nrow(profile) - 1)]
      deltaY <- profile$value[2:nrow(profile)] - profile$value[1:(nrow(profile) - 1)]
      slopes <- deltaY / deltaX
      
      # Compute where prior and posterior slopes intersect
      slopes <- c(
        slopes[1] + (slopes[2] - slopes[3]),
        slopes,
        slopes[length(slopes)] - (slopes[length(slopes) - 1] - slopes[length(slopes)])
      )
      
      interceptX <- (profile$value[2:nrow(profile)] -
                       profile$point[2:nrow(profile)] * slopes[3:length(slopes)] -
                       profile$value[1:(nrow(profile) - 1)] +
                       profile$point[1:(nrow(profile) - 1)] * slopes[1:(length(slopes) - 2)]) /
        (slopes[1:(length(slopes) - 2)] - slopes[3:length(slopes)])
      
      # Compute absolute difference between linear interpolation and worst case scenario (which is at the intercept):
      maxError <- abs((profile$value[1:(nrow(profile) - 1)] + (interceptX - profile$point[1:(nrow(profile) - 1)]) * slopes[1:(length(slopes) - 2)]) -
                        (profile$value[1:(nrow(profile) - 1)] + (interceptX - profile$point[1:(nrow(profile) - 1)]) * slopes[2:(length(slopes) - 1)]))
      
      exceed <- which(maxError > tolerance)
      grid <- (profile$point[exceed] + profile$point[exceed + 1]) / 2
    }
    return(profile)
  }
  
  # Compute calibrated and uncalibrated LLRs at time t
  computeAtT <- function(t, cv) {
    writeLines(sprintf("Computing LLRs for period %d", t))
    dataAtT <- data %>%
      filter(.data$t <= !!t) %>%
      group_by(.data$outcomeId, .data$trueEffectSize) %>%
      summarize(
        observed = sum(.data$observed),
        expected = sum(.data$expected),
        .groups = "drop"
      )
    
    # Compute likelihood profiles
    computeProfile <- function(i) {
      return(computeAbstractPoissonLlProfile(
        observed = dataAtT$observed[i],
        expected = dataAtT$expected[i]
      ))
    }
    profiles <- lapply(1:nrow(dataAtT), computeProfile)
    
    # Fit empirical null distribution using likelihood profiles
    idx <- dataAtT$trueEffectSize == 1
    ncProfiles <- profiles[idx]
    null <- EmpiricalCalibration::fitNullNonNormalLl(ncProfiles)
    
    # Compute LLRs and calibrated and uncalibrated p-values
    computeLlrAndP <- function(i) {
      observedGreaterThanExpected <- dataAtT$observed[i] > dataAtT$expected[i]
      if (observedGreaterThanExpected) {
        llr <- dpois(dataAtT$observed[i], dataAtT$observed[i], log = TRUE) -
          dpois(dataAtT$observed[i], dataAtT$expected[i], log = TRUE)
      } else {
        llr <- 0
      }
      calibratedLlr <- suppressMessages(
        EmpiricalCalibration::calibrateLlr(
          null = null,
          likelihoodApproximation = profiles[[i]],
          twoSided = FALSE,
          upper = TRUE
        )
      )
      p <- EmpiricalCalibration:::computePFromLlr(llr, if_else(observedGreaterThanExpected, 1, -1))
      calibratedP <- EmpiricalCalibration:::computePFromLlr(calibratedLlr, if_else(observedGreaterThanExpected, 1, -1))
      return(tibble(
        llr = llr,
        calibratedLlr = calibratedLlr,
        p = p,
        calibratedP = calibratedP
      ))
    }
    llrs <- lapply(1:nrow(dataAtT), computeLlrAndP)
    llrs <- bind_rows(llrs)
    dataAtT <- bind_cols(dataAtT, llrs)
    dataAtT <- dataAtT %>%
      mutate(
        nullMean = null[1],
        nullSd = null[2],
        t = !!t
      )
    return(dataAtT)
  }
  
  # Simulate data per control:
  trueEffectSize <- rep(parameters$trueEffectSizes, parameters$controlsPerEffectSize)
  systematicError <- exp(rnorm(
    n = length(trueEffectSize),
    mean = parameters$null$mu,
    sd = parameters$null$sigma
  ))
  # Simulate across time intervals:
  simulateDataPerControl <- function(i) {
    lambdaPerInterval <- parameters$n * parameters$baseLineRate * trueEffectSize[i] * systematicError[i] / parameters$looks
    data <- tibble(
      t = 1:parameters$looks,
      outcomeId = rep(i, parameters$looks),
      observed = rpois(parameters$looks, lambdaPerInterval),
      expected = rep(parameters$n * parameters$baseLineRate / parameters$looks, parameters$looks),
      trueEffectSize = rep(trueEffectSize[i], parameters$looks)
    )
  }
  data <- lapply(1:length(trueEffectSize), simulateDataPerControl)
  data <- bind_rows(data)
  
  # Compute LLRs per control and time period
  llrData <- lapply(1:parameters$looks, computeAtT, cv = cv)
  llrData <- bind_rows(llrData)
  
  # Compute critical values per control, and whether and LLR exceeds CV
  computeCvAndSignals <- function(subset) {
    sampleSizeUpperLimit <- max(subset$observed, na.rm = TRUE)
    events <- subset$observed
    if (length(events) > 1) {
      events[2:length(events)] <- events[2:length(events)] - events[1:(length(events) - 1)]
      events <- events[events != 0]
    }
    if (length(events) == 0) {
      cv <- Inf
      signalCalibrated <- FALSE
    } else {
      cv <- EmpiricalCalibration::computeCvPoisson(
        groupSizes = rep(max(subset$expected) / parameters$looks, parameters$looks),
        minimumEvents = 1,
        sampleSize = 1e5,
        alpha = parameters$alpha
      )
      signalCalibrated <- FALSE
      for (i in 1:nrow(subset)) {
        cvT <- EmpiricalCalibration::computeCvPoisson(
          groupSizes = events,
          minimumEvents = 1,
          sampleSize = 1e5,
          alpha = parameters$alpha,
          nullMean = subset$nullMean[i],
          nullSd = subset$nullSd[i]
        )
        if (subset$llr[i] > cvT) {
          signalCalibrated <- TRUE
          break
        }
      }
    }
    return(tibble(
      outcomeId = subset$outcomeId[1],
      trueEffectSize = subset$trueEffectSize[1],
      exposedOutcomes = max(subset$observed),
      cv = cv,
      signal = any(subset$llr > cv),
      signalCalibrated = signalCalibrated,
      signalP = any(subset$p < parameters$alpha),
      signalCalibratedP = any(subset$calibratedP < parameters$alpha)
    ))
  }
  aggregatedAcrossTime <- lapply(split(llrData, llrData$outcomeId), computeCvAndSignals)
  aggregatedAcrossTime <- do.call(rbind, aggregatedAcrossTime)
  aggregatedAcrossTime <- aggregatedAcrossTime %>%
    mutate(
      signal = if_else(is.na(.data$signal), FALSE, .data$signal),
      signalCalibrated = if_else(is.na(.data$signalCalibrated), FALSE, .data$signalCalibrated),
      signalP = if_else(is.na(.data$signalP), FALSE, .data$signalP),
      signalCalibratedP = if_else(is.na(.data$signalCalibratedP), FALSE, .data$signalCalibratedP)
    )
  
  error <- aggregatedAcrossTime %>%
    group_by(.data$trueEffectSize) %>%
    summarize(
      error = mean(.data$signal),
      errorCalibrated = mean(.data$signalCalibrated),
      errorP = mean(.data$signalP),
      errorCalibratedP = mean(.data$signalCalibratedP),
      meanEvents = mean(.data$exposedOutcomes),
      minEvents = min(.data$exposedOutcomes),
      q25Events = quantile(.data$exposedOutcomes, 0.25),
      medianEvents = quantile(.data$exposedOutcomes, 0.5),
      q75Events = quantile(.data$exposedOutcomes, 0.75),
      maxEvents = max(.data$exposedOutcomes)
    ) %>%
    mutate(type = if_else(.data$trueEffectSize == 1, 1, 2)) %>%
    mutate(
      error = if_else(.data$type == 1, .data$error, 1 - .data$error),
      errorCalibrated = if_else(.data$type == 1, .data$errorCalibrated, 1 - .data$errorCalibrated),
      errorP = if_else(.data$type == 1, .data$errorP, 1 - .data$errorP),
      errorCalibratedP = if_else(.data$type == 1, .data$errorCalibratedP, 1 - .data$errorCalibratedP)
    )
  return(error)
}


# Conditional Poisson (SCCS) -------------------------------------------------------------

#' Create parameters for the SCCS simulation
#'
#' @param n                     Number of subjects to simulate.
#' @param tar                   The time-at-risk following the exposure.
#' @param maxT                  The maximum number of days of follow-up.
#' @param trueEffectSizes       A vector of true effect sizes (IRRs) to simulate.
#' @param controlsPerEffectSize Number of control exposure-outcomes per true effect size.
#' @param looks                 Number of (equally spaced) looks at the data.
#' @param null                  A list containing the parameters of the true null distribution.
#' @param alpha                 The target type 1 error rate.
#'
#' @return
#' A list containing the specified parameters.
#'
#' @export
createSccsSimulationParameters <- function(n = 100,
                                           tar = 10,
                                           maxT = 100,
                                           trueEffectSizes = c(1, 1.5, 2, 4),
                                           controlsPerEffectSize = 50,
                                           looks = 10,
                                           null = list(mu = 0.2, sigma = 0.2),
                                           alpha = 0.05) {
  parameters <- list(
    n = n,
    tar = tar,
    maxT = maxT,
    trueEffectSizes = trueEffectSizes,
    controlsPerEffectSize = controlsPerEffectSize,
    looks = looks,
    null = null,
    alpha = alpha
  )
  return(parameters)
}

#' Run an SCCS simulation
#'
#' @param parameters Parameters as created using the
#'                   \code{\link{createSccsSimulationParameters}} function.
#' @iterations       Number of times to repeat the simulation.
#' @threads          Number of parallel threads to use.
#'
#' @return
#' A tibble with results of the simulation runs.
#'
#' @export
simulateSccs <- function(parameters, iterations = 100, threads = parallel::detectCores()) {
  cluster <- ParallelLogger::makeCluster(threads)
  on.exit(ParallelLogger::stopCluster(cluster))
  ParallelLogger::clusterRequire(cluster, "dplyr")
  ParallelLogger::clusterRequire(cluster, "survival")
  runs <- ParallelLogger::clusterApply(cluster, 1:iterations, simulateSingleSccs, parameters = parameters)
  runs <- bind_rows(runs)
  return(runs)
}

simulateSingleSccs <- function(seed, parameters) {
  set.seed(seed)
  
  computeAtT <- function(t) {
    writeLines(sprintf("Computing at time t = %d", t))
    dataAtT <- data %>%
      filter(.data$tExposure < t & .data$tOutcome < t)
    exposedOutcome <- dataAtT$tOutcome > dataAtT$tExposure & dataAtT$tOutcome < dataAtT$tExposure + parameters$tar
    unexposedOutcome <- dataAtT$tOutcome & !exposedOutcome
    exposedTime <- pmax(pmin(t - dataAtT$tExposure, parameters$tar), 0)
    unexposedTime <- t - exposedTime
    dataAtT <- tibble(
      time = c(exposedTime, unexposedTime),
      outcome = c(exposedOutcome, unexposedOutcome),
      exposure = c(rep(1, nrow(dataAtT)), rep(0, nrow(dataAtT))),
      stratumId = rep(rep(1:nrow(dataAtT), 2)),
      outcomeId = rep(dataAtT$outcomeId, 2),
      trueEffectSize = rep(dataAtT$trueEffectSize, 2)
    )
    dataAtT <- dataAtT %>%
      filter(.data$time > 0)
    
    # Compute likelihood profiles
    computeProfileAndsummaryData <- function(outcomeData) {
      cyclopsData <- Cyclops::createCyclopsData(outcome ~ exposure + strata(stratumId) + offset(log(time)), modelType = "cpr", data = outcomeData)
      fit <- Cyclops::fitCyclopsModel(cyclopsData)
      if (fit$return_flag == "SUCCESS" && coef(fit) > 0) {
        llNull <- Cyclops::getCyclopsProfileLogLikelihood(
          object = fit,
          parm = "exposure",
          x = 0,
          includePenalty = FALSE
        )$value
        llr <- fit$log_likelihood - llNull
      } else {
        llr <- 0
      }
      llProfile <- Cyclops::getCyclopsProfileLogLikelihood(
        object = fit,
        parm = "exposure",
        bounds = c(log(0.1), log(10))
      )
      summaryData <- tibble(
        outcomeId = outcomeData$outcomeId[1],
        trueEffectSize = outcomeData$trueEffectSize[1],
        exposedTime = sum(outcomeData$time[outcomeData$exposure == 1]),
        unexposedTime = sum(outcomeData$time[outcomeData$exposure == 0]),
        exposedOutcomes = sum(outcomeData$outcome[outcomeData$exposure == 1]),
        events = sum(outcomeData$outcome),
        llr = llr
      )
      return(list(
        llProfile = llProfile,
        summaryData = summaryData
      ))
    }
    profilesAndSummaryData <- lapply(split(dataAtT, dataAtT$outcomeId), computeProfileAndsummaryData)
    
    # Fit empirical null distribution using likelihood profiles
    profilesNcs <- lapply(profilesAndSummaryData, function(x) if (x$summaryData$trueEffectSize == 1) {
      return(x$llProfile)
    } else {
      return(NULL)
    })
    profilesNcs <- profilesNcs[lengths(profilesNcs) != 0]
    null <- EmpiricalCalibration::fitNullNonNormalLl(profilesNcs)
    
    # Compute estimates
    computeEstimates <- function(profileAndSummaryData) {
      if (is.null(profileAndSummaryData$llProfile)) {
        calibratedLlr <- 0
      } else {
        calibratedLlr <- suppressMessages(
          EmpiricalCalibration::calibrateLlr(
            null = null,
            likelihoodApproximation = profileAndSummaryData$llProfile,
            twoSided = FALSE,
            upper = TRUE
          )
        )
      }
      llr <- profileAndSummaryData$summaryData$llr
      p <- EmpiricalCalibration:::computePFromLlr(llr, if_else(llr > 0, 1, -1))
      calibratedP <- EmpiricalCalibration:::computePFromLlr(calibratedLlr, if_else(llr > 0, 1, -1))
      profileAndSummaryData$summaryData %>%
        mutate(
          calibratedLlr = !!calibratedLlr,
          p = !!p,
          calibratedP = !!calibratedP,
          nullMean = null[1],
          nullSd = null[2]
        ) %>%
        return()
    }
    llrs <- lapply(profilesAndSummaryData, computeEstimates)
    llrs <- bind_rows(llrs) %>%
      mutate(t = !!t)
    return(llrs)
  }
  # Simulate data per control:
  trueEffectSize <- rep(parameters$trueEffectSizes, parameters$controlsPerEffectSize)
  nControls <- length(trueEffectSize)
  systematicError <- exp(rnorm(
    n = nControls,
    mean = parameters$null$mu,
    sd = parameters$null$sigma
  ))
  tExposure <- runif(parameters$n, 0, parameters$maxT)
  
  # Simulate across time intervals:
  simulateDataPerControl <- function(i) {
    irr <- trueEffectSize[i] * systematicError[i]
    tOutcome <- runif(parameters$n, 0, parameters$maxT + (1 - irr) * parameters$tar)
    idxAfterTar <- tOutcome > tExposure + (parameters$tar * irr)
    idxDuringTar <- tOutcome > tExposure & tOutcome <= tExposure + (parameters$tar * irr)
    tOutcome[idxAfterTar] <- tOutcome[idxAfterTar] - ((1 - irr) * parameters$tar)
    tOutcome[idxDuringTar] <- tExposure[idxDuringTar] + ((tOutcome[idxDuringTar] - tExposure[idxDuringTar]) / irr)
    return(tibble(
      outcomeId = i,
      trueEffectSize = trueEffectSize[i],
      tExposure = tExposure,
      tOutcome = tOutcome
    ))
  }
  data <- lapply(1:nControls, simulateDataPerControl)
  data <- bind_rows(data)
  
  # Compute LLRs per control and time period
  t <- seq(0, parameters$maxT, length.out = parameters$looks + 1)[-1]
  llrData <- lapply(t, computeAtT)
  llrData <- bind_rows(llrData)
  
  # Compute critical values per control, and whether and LLR exceeds CV
  computeCvAndSignals <- function(subset) {
    sampleSizeUpperLimit <- max(subset$events, na.rm = TRUE)
    events <- subset$events
    if (length(events) > 1) {
      events[2:length(events)] <- events[2:length(events)] - events[1:(length(events) - 1)]
      events <- events[events != 0]
    }
    if (length(events) == 0) {
      cv <- Inf
      signalCalibrated <- FALSE
    } else {
      cv <- EmpiricalCalibration::computeCvBinomial(
        groupSizes = events,
        z = max(subset$unexposedTime) / max(subset$exposedTime),
        minimumEvents = 1,
        sampleSize = 1e5,
        alpha = parameters$alpha
      )
      signalCalibrated <- FALSE
      for (i in 1:nrow(subset)) {
        cvT <- EmpiricalCalibration::computeCvBinomial(
          groupSizes = events,
          z = max(subset$unexposedTime) / max(subset$exposedTime),
          minimumEvents = 1,
          sampleSize = 1e5,
          alpha = parameters$alpha,
          nullMean = subset$nullMean[i],
          nullSd = subset$nullSd[i]
        )
        if (subset$llr[i] > cvT) {
          signalCalibrated <- TRUE
          break
        }
      }
    }
    return(tibble(
      outcomeId = subset$outcomeId[1],
      trueEffectSize = subset$trueEffectSize[1],
      exposedOutcomes = max(subset$exposedOutcomes),
      cv = cv,
      signal = any(subset$llr > cv),
      signalCalibrated = signalCalibrated,
      signalP = any(subset$p < parameters$alpha),
      signalCalibratedP = any(subset$calibratedP < parameters$alpha)
    ))
  }
  aggregatedAcrossTime <- lapply(split(llrData, llrData$outcomeId), computeCvAndSignals)
  aggregatedAcrossTime <- do.call(rbind, aggregatedAcrossTime)
  aggregatedAcrossTime <- aggregatedAcrossTime %>%
    mutate(
      signal = if_else(is.na(.data$signal), FALSE, .data$signal),
      signalCalibrated = if_else(is.na(.data$signalCalibrated), FALSE, .data$signalCalibrated),
      signalP = if_else(is.na(.data$signalP), FALSE, .data$signalP),
      signalCalibratedP = if_else(is.na(.data$signalCalibratedP), FALSE, .data$signalCalibratedP)
    )
  
  
  error <- aggregatedAcrossTime %>%
    group_by(.data$trueEffectSize) %>%
    summarize(
      error = mean(.data$signal),
      errorCalibrated = mean(.data$signalCalibrated),
      errorP = mean(.data$signalP),
      errorCalibratedP = mean(.data$signalCalibratedP),
      meanEvents = mean(.data$exposedOutcomes),
      minEvents = min(.data$exposedOutcomes),
      q25Events = quantile(.data$exposedOutcomes, 0.25),
      medianEvents = quantile(.data$exposedOutcomes, 0.5),
      q75Events = quantile(.data$exposedOutcomes, 0.75),
      maxEvents = max(.data$exposedOutcomes)
    ) %>%
    mutate(type = if_else(.data$trueEffectSize == 1, 1, 2)) %>%
    mutate(
      error = if_else(.data$type == 1, .data$error, 1 - .data$error),
      errorCalibrated = if_else(.data$type == 1, .data$errorCalibrated, 1 - .data$errorCalibrated),
      errorP = if_else(.data$type == 1, .data$errorP, 1 - .data$errorP),
      errorCalibratedP = if_else(.data$type == 1, .data$errorCalibratedP, 1 - .data$errorCalibratedP)
    )
  return(error)
}
