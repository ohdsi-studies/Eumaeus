library(dplyr)
library(survival)
library(ggplot2)
library(ParallelLogger)
library(Cyclops)
library(EmpiricalCalibration)
# setwd("C:/Users/mschuemi/git/CalibrationMaxSPRTManuscript/SupplementaryMaterials")
source("SimulationFunctions.R")

# Historical comparator simulations --------------------------------------------------------

ns <- c(1e5, 1e6)
nulls <- list(list(mu = 0.0, sigma = 0.0),
             list(mu = 0.0, sigma = 0.2),
             list(mu = 0.2, sigma = 0.2))

# Run simulations
for (n in ns) {
  for (null in nulls) {
    fileName <- sprintf("simHc_n%d_mu_%s_sigma_%s.rds", n, null$mu, null$sigma)
    if (!file.exists(fileName)) {
      message(sprintf("Creating simulation file %s", fileName))
      parameters <- createAbstractPoissonSimulationParameters(n = n, null = null)
      runs <- simulateAbstractPoisson(parameters, iterations = 100, threads = 20)    
      saveRDS(runs, fileName)
    }
  }
}

# Combine simulations in one table and plot
data <- tibble()
for (n in ns) {
  for (null in nulls) {
    fileName <- sprintf("simHc_n%d_mu_%s_sigma_%s.rds", n, null$mu, null$sigma)
    data <- rbind(data, readRDS(fileName) %>%
                    mutate(n = sprintf("Number of subjects = %s", format(!!n, scientific = FALSE, big.mark = ",")),
                           null = sprintf("Systematic error: mean = %0.2f, SD = %0.2f", null$mu, null$sigma)))
  }
}
data %>%
  group_by(n) %>%
  summarise(mean(meanEvents))

labels <- tibble(label = c("Type 1", "Type 2 at\nIRR = 1.5", "Type 2 at\nIRR = 2.0", "Type 2 at\nIRR = 4.0"),
                 x = 1:4,
                 trueEffectSize = c(1, 1.5, 2, 4))

prettyNames <- tibble(name = c("error", "errorCalibrated", "errorP", "errorCalibratedP"),
                    prettyName = c("Uncalibrated MaxSPRT", "Calibrated MaxSPRT", "Uncalibrated, no adj. for sequential testing", "Calibrated, no adj. for sequential testing"),
                    xOffset = c(0.1, 0.3, -0.3, -0.1))

metrics <- data %>% 
  tidyr::pivot_longer(cols = c("error", "errorCalibrated", "errorP", "errorCalibratedP")) %>%
  filter(!is.na(.data$value)) %>%
  inner_join(prettyNames, by = "name") %>%
  inner_join(labels, by = "trueEffectSize") %>%
  mutate(x = .data$x  + .data$xOffset)

colors <- RColorBrewer::brewer.pal(4, "Dark2")

metrics$prettyName <- factor(metrics$prettyName, levels = prettyNames$prettyName[order(prettyNames$xOffset)])

ggplot(metrics, aes(x = .data$x, y = .data$value)) +
  geom_violin(aes(color = .data$prettyName, fill = .data$prettyName, group = .data$x), size = 0.5, scale = "width", alpha = 0.3) +
  scale_color_manual(values = colors) +
  scale_fill_manual(values = colors) +
  geom_vline(xintercept = seq(1.5, 3.5, by = 1)) +
  geom_segment(x = 0, y = 0.05, xend = 1.5, yend = 0.05, linetype = "dashed") +
  scale_x_continuous(limits = c(0.6, 4.5), breaks = labels$x, labels = labels$label, expand = c(0.01, 0.01)) +
  scale_y_continuous("Error", limits = c(0, 1)) +
  facet_grid(.data$n ~ .data$null) +
  theme(panel.background = element_blank(),
        panel.grid.major.y = element_line(colour = "#CCCCCC"),
        panel.grid.major.x = element_blank(),
        axis.ticks.x = element_blank(),
        axis.title.x = element_blank(),
        legend.title = element_blank(),
        panel.border = element_rect(color = "black", fill = NA),
        strip.background = element_blank(),
        legend.position = "top")
ggsave("simErrorHc.pdf", width = 9, height = 6, dpi = 400)

data %>%
  group_by(.data$trueEffectSize, .data$n) %>%
  summarise(events = mean(.data$meanEvents))


# SCCS simulations -------------------------

ns <- c(100, 1000)
nulls <- list(list(mu = 0.0, sigma = 0.0),
              list(mu = 0.0, sigma = 0.2),
              list(mu = 0.2, sigma = 0.2))

# Run simulations
for (n in ns) {
  for (null in nulls) {
    fileName <- sprintf("simSccs_n%d_mu_%s_sigma_%s.rds", n, null$mu, null$sigma)
    if (!file.exists(fileName)) {
      message(sprintf("Creating simulation file %s", fileName))
      parameters <- createSccsSimulationParameters(n = n, null = null)
      runs <- simulateSccs(parameters, iterations = 100, threads = 20)    
      saveRDS(runs, fileName)
    }
  }
}

# Combine simulations in one table and plot
data <- tibble()
for (n in ns) {
  for (null in nulls) {
    fileName <- sprintf("simSccs_n%d_mu_%s_sigma_%s.rds", n, null$mu, null$sigma)
    data <- rbind(data, readRDS(fileName) %>%
                    mutate(n = sprintf("Number of subjects = %s", format(!!n, scientific = FALSE, big.mark = ",")),
                           null = sprintf("Systematic error: mean = %0.2f, SD = %0.2f", null$mu, null$sigma)))
  }
}
data %>%
  group_by(n) %>%
  summarise(mean(meanEvents))


labels <- tibble(label = c("Type 1", "Type 2 at\nIRR = 1.5", "Type 2 at\nIRR = 2.0", "Type 2 at\nIRR = 4.0"),
                 x = 1:4,
                 trueEffectSize = c(1, 1.5, 2, 4))

prettyNames <- tibble(name = c("error", "errorCalibrated", "errorP", "errorCalibratedP"),
                      prettyName = c("Uncalibrated MaxSPRT", "Calibrated MaxSPRT", "Uncalibrated, no adj. for sequential testing", "Calibrated, no adj. for sequential testing"),
                      xOffset = c(0.1, 0.3, -0.3, -0.1))

metrics <- data %>% 
  tidyr::pivot_longer(cols = c("error", "errorCalibrated", "errorP", "errorCalibratedP")) %>%
  filter(!is.na(.data$value)) %>%
  inner_join(prettyNames, by = "name") %>%
  inner_join(labels, by = "trueEffectSize") %>%
  mutate(x = .data$x  + .data$xOffset)

colors <- RColorBrewer::brewer.pal(4, "Dark2")

metrics$prettyName <- factor(metrics$prettyName, levels = prettyNames$prettyName[order(prettyNames$xOffset)])

ggplot(metrics, aes(x = .data$x, y = .data$value)) +
  geom_violin(aes(color = .data$prettyName, fill = .data$prettyName, group = .data$x), size = 0.5, scale = "width", alpha = 0.3) +
  scale_color_manual(values = colors) +
  scale_fill_manual(values = colors) +
  geom_vline(xintercept = seq(1.5, 3.5, by = 1)) +
  geom_segment(x = 0, y = 0.05, xend = 1.5, yend = 0.05, linetype = "dashed") +
  scale_x_continuous(limits = c(0.6, 4.5), breaks = labels$x, labels = labels$label, expand = c(0.01, 0.01)) +
  scale_y_continuous("Error", limits = c(0, 1)) +
  facet_grid(.data$n ~ .data$null) +
  theme(panel.background = element_blank(),
        panel.grid.major.y = element_line(colour = "#CCCCCC"),
        panel.grid.major.x = element_blank(),
        axis.ticks.x = element_blank(),
        axis.title.x = element_blank(),
        legend.title = element_blank(),
        panel.border = element_rect(color = "black", fill = NA),
        strip.background = element_blank(),
        legend.position = "top")
ggsave("simErrorSccs.pdf", width = 9, height = 6, dpi = 400)

