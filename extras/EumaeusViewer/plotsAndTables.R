library(ggplot2)
library(dplyr)

plotScatter <- function(d) {
  d$Significant <- d$ci95Lb > d$effectSize | d$ci95Ub < d$effectSize
  
  temp1 <- aggregate(Significant ~ Group, data = d, length)
  temp2 <- aggregate(Significant ~ Group, data = d, mean)
  
  temp1$nLabel <- paste0(formatC(temp1$Significant, big.mark = ","), " estimates")
  temp1$Significant <- NULL
  
  temp2$meanLabel <- paste0(formatC(100 * (1 - temp2$Significant), digits = 1, format = "f"),
                            "% of CIs includes ",
                            substr(as.character(temp2$Group), start = 21, stop = nchar(as.character(temp2$Group))))
  temp2$Significant <- NULL
  dd <- merge(temp1, temp2)
  # print(substr(as.character(dd$Group), start = 20, stop = nchar(as.character(dd$Group))))
  dd$tes <- as.numeric(substr(as.character(dd$Group), start = 21, stop = nchar(as.character(dd$Group))))
  
  breaks <- c(0.25, 0.5, 1, 2, 4, 6, 8, 10)
  theme <- element_text(colour = "#000000", size = 14)
  themeRA <- element_text(colour = "#000000", size = 14, hjust = 1)
  themeLA <- element_text(colour = "#000000", size = 14, hjust = 0)
  alpha <- 1 - min(0.95*(nrow(d)/nrow(dd)/50000)^0.1, 0.95)
  plot <- ggplot(d, aes(x = logRr, y= seLogRr), environment = environment()) +
    geom_vline(xintercept = log(breaks), colour = "#CCCCCC", lty = 1, size = 0.5) +
    geom_abline(aes(intercept = (-log(tes))/qnorm(0.025), slope = 1/qnorm(0.025)), colour = rgb(0.8, 0, 0), linetype = "dashed", size = 1, alpha = 0.5, data = dd) +
    geom_abline(aes(intercept = (-log(tes))/qnorm(0.975), slope = 1/qnorm(0.975)), colour = rgb(0.8, 0, 0), linetype = "dashed", size = 1, alpha = 0.5, data = dd) +
    geom_point(size = 2, color = rgb(0, 0, 0, alpha = 0.05), alpha = alpha, shape = 16) +
    geom_hline(yintercept = 0) +
    geom_label(x = log(0.26), y = 0.95, alpha = 1, hjust = "left", aes(label = nLabel), size = 5, data = dd) +
    geom_label(x = log(0.26), y = 0.8, alpha = 1, hjust = "left", aes(label = meanLabel), size = 5, data = dd) +
    scale_x_continuous("Estimated effect size", limits = log(c(0.25, 10)), breaks = log(breaks), labels = breaks) +
    scale_y_continuous("Standard Error", limits = c(0, 1)) +
    facet_grid(. ~ Group) +
    theme(panel.grid.minor = element_blank(),
          panel.background = element_blank(),
          panel.grid.major = element_blank(),
          axis.ticks = element_blank(),
          axis.text.y = themeRA,
          axis.text.x = theme,
          axis.title = theme,
          legend.key = element_blank(),
          strip.text.x = theme,
          strip.text.y = theme,
          strip.background = element_blank(),
          legend.position = "none")
  return(plot)
}

plotRocsInjectedSignals <- function(logRr, trueLogRr, showAucs, fileName = NULL) {
  trueLogRrLevels <- unique(trueLogRr)
  trueLogRrLevels <- trueLogRrLevels[order(trueLogRrLevels)]
  allData <- data.frame()
  aucs <- c()
  labels <- c()
  overall <- c()
  for (trueLogRrLevel in trueLogRrLevels) {
    if (trueLogRrLevel != 0 ) {
      data <- data.frame(logRr = logRr[trueLogRr == 0 | trueLogRr == trueLogRrLevel], 
                         trueLogRr = trueLogRr[trueLogRr == 0 | trueLogRr == trueLogRrLevel])
      data$truth <- data$trueLogRr != 0
      label <- paste("True effect size =", exp(trueLogRrLevel))
      roc <- pROC::roc(data$truth, data$logRr, algorithm = 3)
      if (showAucs) {
        aucs <- c(aucs, pROC::auc(roc))
        labels <- c(labels, label)
        overall <- c(overall, FALSE)
      }
      data <- data.frame(sens = roc$sensitivities, 
                         fpRate = 1 - roc$specificities, 
                         label = label,
                         overall = FALSE,
                         stringsAsFactors = FALSE)
      data <- data[order(data$sens, data$fpRate), ]
      allData <- rbind(allData, data)
    }
  }
  # Overall ROC
  data <- data.frame(logRr = logRr, 
                     trueLogRr = trueLogRr)
  data$truth <- data$trueLogRr != 0
  roc <- pROC::roc(data$truth, data$logRr, algorithm = 3)
  if (showAucs) {
    aucs <- c(aucs, pROC::auc(roc))
    labels <- c(labels, "Overall")
    overall <- c(overall, TRUE)
  }
  data <- data.frame(sens = roc$sensitivities, 
                     fpRate = 1 - roc$specificities, 
                     label = "Overall", 
                     overall = TRUE,
                     stringsAsFactors = FALSE)
  data <- data[order(data$sens, data$fpRate), ]
  allData <- rbind(allData, data)
  
  allData$label <- factor(allData$label, levels = c(paste("True effect size =", exp(trueLogRrLevels)), "Overall"))
  # labels <- factor(labels, levels = c("Overall", paste("True effect size =", exp(trueLogRrLevels))))
  breaks <- seq(0, 1, by = 0.2)
  theme <- element_text(colour = "#000000", size = 15)
  themeRA <- element_text(colour = "#000000", size = 15, hjust = 1)
  plot <- ggplot(allData, aes(x = fpRate, y = sens, group = label, color = label, fill = label)) +
    geom_vline(xintercept = breaks, colour = "#CCCCCC", lty = 1, size = 0.5) +
    geom_hline(yintercept = breaks, colour = "#CCCCCC", lty = 1, size = 0.5) +
    geom_abline(intercept = 0, slope = 1) +
    geom_line(aes(linetype = overall), alpha = 0.5, size = 1) +
    scale_x_continuous("1 - specificity", breaks = breaks, labels = breaks) +
    scale_y_continuous("Sensitivity", breaks = breaks, labels = breaks) +
    labs(color = "True effect size", linetype = "Overall") +
    theme(panel.grid.minor = element_blank(),
          panel.background = element_blank(),
          panel.grid.major = element_blank(),
          axis.ticks = element_blank(),
          axis.text.y = themeRA,
          axis.text.x = theme,
          axis.title = theme,
          legend.key = element_blank(),
          strip.text.x = theme,
          strip.text.y = theme,
          strip.background = element_blank(),
          legend.position = "right",
          legend.text = theme,
          legend.title = theme)
  
  
  if (showAucs) {
    aucs <- data.frame(auc = aucs, label = labels) 
    aucs <- aucs[order(aucs$label), ]
    for (i in 1:nrow(aucs)) {
      label <- paste0(aucs$label[i], ": AUC = ", format(round(aucs$auc[i], 2), nsmall = 2))
      plot <- plot + geom_text(label = label, x = 1, y = 0.4 - (i*0.1), hjust = 1, color = "#000000", size = 5)
    }
  }
  if (!is.null(fileName))
    ggsave(fileName, plot, width = 5.5, height = 4.5, dpi = 400)
  return(plot)
}

plotOverview <- function(metrics, metric, strataSubset, calibrated) {
  yLabel <- paste0(metric, if (calibrated == "Calibrated") " after empirical calibration" else "")
  point <- scales::format_format(big.mark = " ", decimal.mark = ".", scientific = FALSE)
  fiveColors <- c(
    "#781C86",
    "#83BA70",
    "#D3AE4E",
    "#547BD3",
    "#DF4327"
  )
  plot <- ggplot2::ggplot(metrics, ggplot2::aes(x = x, y = metric, color = tidyMethod)) +
    ggplot2::geom_vline(xintercept = 0.5 + 0:nrow(strataSubset), linetype = "dashed") +
    ggplot2::geom_point(size = 4.5, alpha = 0.5, shape = 16) +
    ggplot2::scale_x_continuous("Stratum", breaks = strataSubset$x, labels = strataSubset$stratum) +
    ggplot2::scale_colour_manual(values = fiveColors) +
    ggplot2::facet_grid(database~., scales = "free_y") +
    ggplot2::theme(panel.grid.minor = ggplot2::element_blank(),
                   panel.background = ggplot2::element_rect(fill = "#F0F0F0F0", colour = NA),
                   panel.grid.major.x = ggplot2::element_blank(),
                   panel.grid.major.y = ggplot2::element_line(colour = "#CCCCCC"),
                   axis.ticks = ggplot2::element_blank(),
                   legend.position = "top",
                   legend.title = ggplot2::element_blank(),
                   text = ggplot2::element_text(size = 20))
  if (metric %in% c("Mean precision (1/SE^2)", "Mean squared error (MSE)")) {
    plot <- plot + ggplot2::scale_y_log10(yLabel, labels = point)
  } else {
    plot <- plot + ggplot2::scale_y_continuous(yLabel, labels = point)
  }
  return(plot)
}


addTrueEffectSize <- function(estimates, negativeControlOutcome, positiveControlOutcome) {
  allControls <- bind_rows(negativeControlOutcome %>%
                             mutate(effectSize = 1) %>%
                             select(.data$outcomeId, .data$outcomeName, .data$effectSize),
                           positiveControlOutcome %>%
                             select(.data$outcomeId, .data$outcomeName, .data$effectSize))
  estimates <- estimates %>%
    inner_join(allControls, by = "outcomeId")
  return(estimates)
}

computeEffectEstimateMetrics <- function(estimates, trueRr = "Overall") {
  if (!"effectSize" %in% colnames(estimates))
    stop("Must add column 'effectSize' to estimates (e.g. using addTrueEffectSize())")
  
  if (trueRr == "Overall") {
    forEval <- estimates
  } else if (trueRr == ">1") {
    forEval <- estimates[estimates$effectSize > 1, ]
  } else {
    forEval <- estimates[estimates$effectSize == as.numeric(trueRr), ]
  }
  nonEstimable <- round(mean(forEval$seLogRr >= 99), 2)
  mse <- round(mean((forEval$logRr - log(forEval$effectSize))^2), 2)
  coverage <- round(mean(forEval$ci95Lb < forEval$effectSize & forEval$ci95Ub > forEval$effectSize), 2)
  meanP <- round(-1 + exp(mean(log(1 + (1/(forEval$seLogRr^2))))), 2)
  if (trueRr == "Overall") {
    roc <- pROC::roc(forEval$effectSize > 1, forEval$logRr, algorithm = 3)
    auc <- round(pROC::auc(roc), 2)
    type1 <- round(mean(forEval$p[forEval$effectSize == 1] < 0.05), 2)
    type2 <- round(mean(forEval$p[forEval$effectSize > 1] >= 0.05), 2)
  } else if (trueRr == "1") {
    roc <- NA
    auc <- NA
    type1 <- round(mean(forEval$p < 0.05), 2)  
    type2 <- NA
  } else {
    if (trueRr == ">1") {
      negAndPos <- estimates[estimates$effectSize > 1 | estimates$effectSize == 1, ]
    } else {
      negAndPos <- estimates[estimates$effectSize == as.numeric(trueRr) | estimates$effectSize == 1, ]
    }
    roc <- pROC::roc(negAndPos$effectSize > 1, negAndPos$logRr, algorithm = 3)
    auc <- round(pROC::auc(roc), 2)
    type1 <- NA
    type2 <- round(mean(forEval$p[forEval$effectSize > 1] >= 0.05), 2)  
  }
  
  return(tibble(method = estimates$method[1],
                analysisId = estimates$analysisId[1],
                auc = auc, 
                coverage = coverage, 
                meanP = meanP, 
                mse = mse, 
                type1 = type1, 
                type2 = type2, 
                nonEstimable = nonEstimable))
}

computeMaxSprtMetricsPerPeriod <- function(estimates) {
  timeToSignal <- estimates %>%
    mutate(signal = .data$llr > .data$criticalValue) %>%
    filter(.data$signal) %>%
    group_by(.data$outcomeId) %>%
    summarize(firstSignalPeriodId = min(.data$periodId))
  
  timeToSignal <- estimates %>%
    distinct(.data$outcomeId, .data$effectSize) %>%
    left_join(timeToSignal, by = "outcomeId") %>%
    mutate(firstSignalPeriodId = if_else(is.na(.data$firstSignalPeriodId), Inf, as.numeric(.data$firstSignalPeriodId)))
  
  # periodId <- 9
  computeSensSpec <- function(periodId) {
    sensitivity <-  timeToSignal %>%
      filter(.data$effectSize > 1) %>%
      summarise(sensitivity = mean(.data$firstSignalPeriodId <= periodId)) %>%
      pull()
    specificity <-  timeToSignal %>%
      filter(.data$effectSize == 1) %>%
      summarise(specificity = 1 - mean(.data$firstSignalPeriodId <= periodId)) %>%
      pull()
    return(tibble(periodId = periodId,
                  sensitivity = sensitivity,
                  specificity = specificity))
  }
  sensSpec <- lapply(1:max(estimates$periodId), computeSensSpec)
  sensSpec <- bind_rows(sensSpec)
  return(sensSpec)
}

# estimates = split(subset, paste(subset$method, subset$analysisId))[[1]]
# estimates <- subset[estimate$method == "HistoricalComparator" & estimate$analysisId == 1, ]
# estimates = addTrueEffectSize(estimates, negativeControlOutcome, positiveControlOutcome)
computeMaxSprtMetrics <- function(estimates, trueRr = "Overall") {
  if (!"effectSize" %in% colnames(estimates))
    stop("Must add column 'effectSize' to estimates (e.g. using addTrueEffectSize())")
  
  if (trueRr == "Overall") {
    forEval <- estimates
  } else if (trueRr == ">1") {
    forEval <- estimates[estimates$effectSize > 1, ]
  } else {
    forEval <- estimates[estimates$effectSize == 1 | estimates$effectSize == as.numeric(trueRr), ]
  }
  
  metricsPerPeriod <- computeMaxSprtMetricsPerPeriod(forEval)
  firstPeriod80Sens <- metricsPerPeriod %>%
    filter(.data$sensitivity >= 0.80) %>%
    summarize(periodId = min(.data$periodId)) %>%
    pull()
  if (is.infinite(firstPeriod80Sens)) {
    result <- tibble(mehod = estimates$method[1],
                     analysisId = estimates$analysisId[1],
                     firstPeriod80Sens = NA,
                     sensitivity = NA,
                     specificity = NA)
  } else {
    result <- metricsPerPeriod %>%
      filter(.data$periodId == firstPeriod80Sens) %>%
      transmute(mehod = estimates$method[1],
             analysisId = estimates$analysisId[1],
             firstPeriod80Sens = !!firstPeriod80Sens,
             sensitivity = round(.data$sensitivity, 2),
             specificity = round(.data$specificity, 2))
  }
  return(result)
}

computeTrueRr <- function(estimates, negativeControlOutcome, positiveControlOutcome) {
  estimates <- addTrueEffectSize(estimates, negativeControlOutcome, positiveControlOutcome)
  
  pcs <- estimates %>%
    inner_join(select(positiveControlOutcome, .data$negativeControlId, .data$outcomeId), by = "outcomeId") %>%
    inner_join(select(estimates, periodId = .data$periodId, negativeControlId = .data$outcomeId, negativeControlOutcomes = .data$exposureOutcomes ), by = c("negativeControlId", "periodId")) %>%
    mutate(trueEffectSize = .data$exposureOutcomes / .data$negativeControlOutcomes) %>%
    select(-.data$negativeControlId)
  
  
 # pcs[pcs$periodId == 2, c("effectSize", "trueEffectSize", "exposureOutcomes", "negativeControlOutcomes")]
  
}

# d <- estimate[estimate$method == "HistoricalComparator" & estimate$analysisId == 1, ]
# d <- addTrueEffectSize(d, negativeControlOutcome, positiveControlOutcome)
# d$Group <- as.factor(paste("True effect size =", d$effectSize))
# d <- d[d$exposureOutcomes >= 3, ]
plotLlrs <- function(d, trueRr = "Overall") {
  d$Signal <- !is.na(d$llr) & !is.na(d$criticalValue) & d$llr >= d$criticalValue
  d$y <- d$llr + 1
  if (trueRr != "Overall") {
    d <- d[d$effectSize == 1 | d$effectSize == as.numeric(trueRr), ]
  }

  theme <- element_text(colour = "#000000", size = 14)
  themeRA <- element_text(colour = "#000000", size = 14, hjust = 1)
  themeLA <- element_text(colour = "#000000", size = 14, hjust = 0)
  yBreaks <- c(0, 1, 2, 3, 4, 5, 10, 20, 30, 40, 50, 100)
  plot <- ggplot(d, aes(x = .data$periodId, y = .data$y, group = .data$outcomeId), environment = environment()) +
    geom_line(size = 1, color = rgb(0, 0, 0), alpha = 0.5) +
    geom_point(aes(shape = .data$Signal), size = 3, color = rgb(0, 0, 0), fill = rgb(1, 1, 1), alpha = 0.7) +
    scale_shape_manual(name = "Above critical value", values = c(21,16)) +
    scale_x_continuous("Time (Months)", breaks = 1:max(d$periodId), limits = c(1, max(d$periodId))) +
    scale_y_log10("Log Likelihood ratio", breaks = yBreaks + 1, labels = yBreaks) +
    coord_cartesian(ylim = c(1, 101)) +
    facet_grid(. ~ Group) +
    theme(axis.text.y = themeRA,
          axis.text.x = theme,
          axis.title = theme,
          strip.text.x = theme,
          strip.text.y = theme,
          strip.background = element_blank(),
          legend.text = themeLA,
          legend.title = themeLA,
          legend.position = "top")
  return(plot)
}