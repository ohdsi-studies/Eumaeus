library(shiny)
library(DT)
library(dplyr)

source("plotsAndTables.R")

shinyServer(function(input, output, session) {
  
  exposureId <- reactive(
    exposure %>%
      filter(.data$exposureName == input$exposure) %>%
      pull(.data$exposureId)
  )
  
  observe({
    timePeriodSubset <- timePeriod %>%
      filter(.data$exposureId == exposureId()) %>%
      pull(.data$label)
    
    updateSelectInput(session, "period", choices = timePeriodSubset, selected = timePeriodSubset[length(timePeriodSubset)])
  })
  
  periodId <- reactive(
    timePeriod %>%
      filter(.data$label == input$period & .data$exposureId == exposureId()) %>%
      pull(.data$periodId)
  ) 
  
  filterEstimates <- reactive({
    analysisIds <- analysis %>%
      filter(.data$timeAtRisk == input$timeAtRisk) %>%
      pull(.data$analysisId)
    
    subset <- getEstimates(connection = connectionPool,
                           schema = schema,
                           databaseId = input$database,
                           exposureId = exposureId(),
                           periodId = periodId(),
                           analysisIds = analysisIds,
                           methods = input$method,
                           calibrated = input$calibrated == "Calibrated")
    subset <- addTrueEffectSize(subset, negativeControlOutcome, positiveControlOutcome)
    return(subset)
  })
  
  selectedEstimates <- reactive({
    if (is.null(input$performanceMetrics_rows_selected)) {
      return(NULL)
    } 
    subset <- filterEstimates()
    if (nrow(subset) == 0) {
      return(NULL)
    }
    subset <- subset[subset$method == performanceMetrics()$Method[input$performanceMetrics_rows_selected] & 
                       subset$analysisId == performanceMetrics()$'<span title=\"Analysis variant ID\">ID</span>'[input$performanceMetrics_rows_selected], ]
    if (nrow(subset) == 0) {
      return(NULL)
    } 
    return(subset)
  })
  
  output$tableCaption <- renderUI({
    subset <- filterEstimates()
    subset <- unique(subset[, c("exposureId", "outcomeId", "effectSize")])
    ncCount <- sum(subset$effectSize == 1)
    pcCount <- sum(subset$effectSize != 1)
    return(HTML(paste0("<strong>Table S.1</strong> Metrics based on ", ncCount, " negative and ", pcCount, " positive controls.")))
  })
  
  performanceMetrics <- reactive({
    subset <- filterEstimates()
    if (nrow(subset) == 0) {
      return(data.frame())
    }
    combis <- lapply(split(subset, paste(subset$method, subset$analysisId)), 
                     computeEffectEstimateMetrics, 
                     trueRr = input$trueRr)
    combis <- bind_rows(combis)
    colnames(combis) <- c("Method", 
                          "<span title=\"Analysis variant ID\">ID</span>", 
                          "<span title=\"Area under the receiver operator curve\">AUC</span>", 
                          "<span title=\"Coverage of the 95% confidence interval\">Coverage</span>", 
                          "<span title=\"Geometric mean precision (1/SE^2)\">Mean Precision</span>", 
                          "<span title=\"Mean Squared Error\">MSE</span>", 
                          "<span title=\"Type I Error\">Type I error</span>", 
                          "<span title=\"Type II Error\">Type II error</span>", 
                          "<span title=\"Fraction where estimate could not be computed\">Non-estimable</span>")
    return(combis)
  })
  
  output$performanceMetrics <- renderDataTable({
    selection = list(mode = "single", target = "row")
    options = list(pageLength = 10, 
                   searching = FALSE, 
                   lengthChange = TRUE)
    isolate(
      if (!is.null(input$performanceMetrics_rows_selected)) {
        selection$selected = input$performanceMetrics_rows_selected
        options$displayStart = floor(input$performanceMetrics_rows_selected[1] / 10) * 10 
      }
    )
    data <- performanceMetrics()
    if (nrow(data) == 0) {
      return(data)
    }
    table <- DT::datatable(data, selection = selection, options = options, rownames = FALSE, escape = FALSE) 
    
    colors <- c("#b7d3e6", "#b7d3e6", "#b7d3e6", "#f2b4a9", "#f2b4a9", "#f2b4a9", "#f2b4a9")
    mins <- c(0, 0, 0, 0, 0, 0, 0)
    maxs <- c(1, 1, max(data[, 5]), max(data[, 6]), 1, 1, 1)
    for (i in 1:length(colors)) {
      table <- DT::formatStyle(table = table, 
                               columns = i + 2,
                               background = styleColorBar(c(mins[i], maxs[i]), colors[i]),
                               backgroundSize = '98% 88%',
                               backgroundRepeat = 'no-repeat',
                               backgroundPosition = 'center')
    }
    return(table)
  })
  
  output$estimates <- renderPlot({
    subset <- selectedEstimates()
    if (is.null(subset)) {
      return(NULL)
    }  else {
      subset$Group <- as.factor(paste("True hazard ratio =", subset$effectSize))
      return(plotScatter(subset))
    }
  })
  
  output$details <- renderText({
    subset <- selectedEstimates()
    if (is.null(subset)) {
      return(NULL)
    }  else {
      method <- as.character(subset$method[1])
      analysisId <- subset$analysisId[1]
      description <- analysis$description[analysis$method == method & analysis$analysisId == analysisId]
      return(paste0(method , " analysis ", analysisId, ": ", description))
    }
  })
  
  outputOptions(output, "details", suspendWhenHidden = FALSE)
  
  observeEvent(input$showSettings, {
    subset <- selectedEstimates()
    method <- as.character(subset$method[1])
    analysisId <- subset$analysisId[1]
    description <- analysisRef$description[analysisRef$method == method & analysisRef$analysisId == analysisId]
    details <- analysisRef$details[analysisRef$method == method & analysisRef$analysisId == analysisId]
    showModal(modalDialog(
      title = paste0(method , " analysis. ", analysisId, ": ", description),
      pre(details),
      easyClose = TRUE,
      footer = NULL,
      size = "l"
    ))
  })
  
  output$rocCurves <- renderPlot({
    subset <- selectedEstimates()
    if (is.null(subset)) {
      return(NULL)
    } else {
      subset$trueLogRr <- log(subset$effectSize)
      return(plotRocsInjectedSignals(logRr = subset$logRr, trueLogRr = subset$trueLogRr, showAucs = TRUE))
    }
    
  })
  
  output$hoverInfoEstimates <- renderUI({
    # Hover-over adapted from https://gitlab.com/snippets/16220
    subset <- selectedEstimates()
    if (is.null(subset)) {
      return(NULL)
    } 
    subset$Group <- as.factor(paste("True hazard ratio =", subset$effectSize))
    hover <- input$plotHoverInfoEstimates
    
    point <- nearPoints(subset, hover, threshold = 50, maxpoints = 1, addDist = TRUE)
    if (nrow(point) == 0) return(NULL)
    
    
    # calculate point position INSIDE the image as percent of total dimensions
    # from left (horizontal) and from top (vertical)
    left_pct <- (hover$x - hover$domain$left) / (hover$domain$right - hover$domain$left)
    top_pct <- (hover$domain$top - hover$y) / (hover$domain$top - hover$domain$bottom)
    
    # calculate distance from left and bottom side of the picture in pixels
    left_px <- hover$range$left + left_pct * (hover$range$right - hover$range$left)
    top_px <- hover$range$top + top_pct * (hover$range$bottom - hover$range$top)
    
    # create style property fot tooltip
    # background color is set so tooltip is a bit transparent
    # z-index is set so we are sure are tooltip will be on top
    style <- paste0("position:absolute; z-index:100; background-color: rgba(245, 245, 245, 0.85); ",
                    "left:", left_px - 125, "px; top:", top_px - 150, "px; width:250px;")
    
    
    # actual tooltip created as wellPanel
    estimate <- paste0(formatC(exp(point$logRr), digits = 2, format = "f"),
                       " (",
                       formatC(point$ci95Lb, digits = 2, format = "f"),
                       "-",
                       formatC(point$ci95Ub, digits = 2, format = "f"),
                       ")")
    
    text <- paste0("<b> outcome: </b>", point$outcomeName, "<br/>",
                   "<b> estimate: </b>", estimate, "<br/>")
    div(
      style = "position: relative; width: 0; height: 0",
      wellPanel(style = style, p(HTML(text)))
    )
  })
  
  
  
  observeEvent(input$evalTypeInfo, {
    showModal(modalDialog(
      title = "Evaluation type",
      easyClose = TRUE,
      footer = NULL,
      size = "l",
      HTML(evalTypeInfoHtml)
    ))
  })
  
  observeEvent(input$evalTypeInfo2, {
    showModal(modalDialog(
      title = "Evaluation type",
      easyClose = TRUE,
      footer = NULL,
      size = "l",
      HTML(evalTypeInfoHtml)
    ))
  })
  
  observeEvent(input$calibrationInfo, {
    showModal(modalDialog(
      title = "Empirical calibration",
      easyClose = TRUE,
      footer = NULL,
      size = "l",
      HTML(calibrationInfoHtml)
    ))
  })
  
  observeEvent(input$calibrationInfo2, {
    showModal(modalDialog(
      title = "Empirical calibration",
      easyClose = TRUE,
      footer = NULL,
      size = "l",
      HTML(calibrationInfoHtml)
    ))
  })
  
  observeEvent(input$mdrrInfo, {
    showModal(modalDialog(
      title = "Minimum Detectable RR",
      easyClose = TRUE,
      footer = NULL,
      size = "l",
      HTML(mdrrInfoHtml)
    ))
  })
  
  observeEvent(input$mdrrInfo2, {
    showModal(modalDialog(
      title = "Minimum Detectable RR",
      easyClose = TRUE,
      footer = NULL,
      size = "l",
      HTML(mdrrInfoHtml)
    ))
  })
  
  observeEvent(input$databaseInfo, {
    showModal(modalDialog(
      title = "Databases",
      easyClose = TRUE,
      footer = NULL,
      size = "l",
      HTML(databaseInfoHtml)
    ))
  })
  
  observeEvent(input$stratumInfo, {
    showModal(modalDialog(
      title = "Strata",
      easyClose = TRUE,
      footer = NULL,
      size = "l",
      HTML(stratumInfoHtml)
    ))
  })
  
  observeEvent(input$trueRrInfo, {
    showModal(modalDialog(
      title = "True effect size",
      easyClose = TRUE,
      footer = NULL,
      size = "l",
      HTML(trueRrInfoHtml)
    ))
  })
  
  observeEvent(input$methodsInfo, {
    showModal(modalDialog(
      title = "Methods",
      easyClose = TRUE,
      footer = NULL,
      size = "l",
      HTML(methodsInfoHtml)
    ))
  })
  
  observeEvent(input$metricInfo, {
    showModal(modalDialog(
      title = "Metrics",
      easyClose = TRUE,
      footer = NULL,
      size = "l",
      HTML(metricInfoHtml)
    ))
  })
})

