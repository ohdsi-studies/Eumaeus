library(shiny)
library(DT)

shinyUI(
  fluidPage(style = 'width:1500px;',
            titlePanel(
              title = div(img(src = "logo.png", height = 50, width = 50), "Evaluating Use of Methods for Adverse Event Under Surveillance (EUMAEUS)"),
              windowTitle = "EUMAEUS"
            ),
            tabsetPanel(
              tabPanel("About",
                       br(),
                       p("For review purposes only. Do not use.")
              ),
              tabPanel("Effect-size-estimate-based metrics",
                       fluidRow(
                         column(2,
                                selectInput("exposure", label = "Vaccine", choices = exposure$exposureName),
                                selectInput("calibrated", label = div("Empirical calibration:", actionLink("calibrationInfo2", "", icon = icon("info-circle"))), choices = c("Uncalibrated", "Calibrated")),
                                selectInput("database", label = div("Database:", actionLink("databaseInfo", "", icon = icon("info-circle"))), choices = database$databaseId),
                                selectInput("period", label = "Time period", choices = timePeriod$label[timePeriod$exposureId == exposure$exposureId[1]]),
                                selectInput("timeAtRisk", label = "Time at risk", choices = timeAtRisks),
                                selectInput("trueRr", label = div("True effect size:", actionLink("trueRrInfo", "", icon = icon("info-circle"))), choices = trueRrs),
                                checkboxGroupInput("method", label =  div("Methods:", actionLink("methodsInfo", "", icon = icon("info-circle"))), choices = unique(analysis$method), selected = unique(analysis$method))
                         ),
                         column(10,
                                dataTableOutput("performanceMetrics"),
                                uiOutput("tableCaption"),
                                conditionalPanel(condition = "output.details",
                                                 div(style = "display:inline-block", h4(textOutput("details"))), 
                                                 # div(style = "display:inline-block", actionLink("showSettings", "Details")),
                                                 tabsetPanel(
                                                   tabPanel("Estimates", 
                                                            uiOutput("hoverInfoEstimates"),
                                                            plotOutput("estimates", 
                                                                       height = "270px",
                                                                       hover = hoverOpts("plotHoverInfoEstimates", 
                                                                                         delay = 100, 
                                                                                         delayType = "debounce")),
                                                            div(strong("Figure 1.1."),"Estimates with standard errors for the negative and positive controls, stratified by true effect size. Estimates that fall above the red dashed lines have a confidence interval that includes the truth. Hover mouse over point for more information.")),
                                                   tabPanel("ROC curves", 
                                                            plotOutput("rocCurves", 
                                                                       height = "420px"),
                                                            div(strong("Figure 1.2."),"Receiver Operator Characteristics curves for distinguising positive controls from negative controls."))
                                                 )
                                )   
                         )
                       )
              ),
              tabPanel("MaxSPRT-based metrics",
                       fluidRow(
                         column(2,
                                selectInput("exposure2", label = "Vaccine", choices = exposure$exposureName),
                                selectInput("database2", label = div("Database:", actionLink("databaseInfo", "", icon = icon("info-circle"))), choices = database$databaseId),
                                textInput("minOutcomes", label = "Minimum outcomes", value = 1),
                                selectInput("timeAtRisk2", label = "Time at risk", choices = timeAtRisks),
                                selectInput("trueRr2", label = div("True effect size:", actionLink("trueRrInfo", "", icon = icon("info-circle"))), choices = trueRrs),
                                checkboxGroupInput("method2", label =  div("Methods:", actionLink("methodsInfo", "", icon = icon("info-circle"))), choices = unique(analysis$method), selected = unique(analysis$method))
                         ),
                         column(10,
                                dataTableOutput("performanceMetrics2"),
                                uiOutput("table2Caption"),
                                conditionalPanel(condition = "output.details2",
                                                 div(style = "display:inline-block", h4(textOutput("details2"))),
                                                 tabsetPanel(
                                                   tabPanel("Log Likelihood Ratios",
                                                            uiOutput("hoverInfoLlrs"),
                                                            plotOutput("llrs",
                                                                       height = "650px",
                                                                       hover = hoverOpts("plotHoverInfoLlrs",
                                                                                         delay = 100,
                                                                                         delayType = "debounce")),
                                                            div(strong("Figure 2.1."),"Log likelihood ratios (LLR) (left axis) for the negative and positive controls at various points in time, stratified by true effect size. Closed dots indicate the LLR in that period exceeded the critical value. The critical value depends on sample size within and across periods, and is therefore different for each control. The yellow area indicates the cumulative number of vaccinations (right axis). Hover mouse over point for more information.")),
                                                   tabPanel("Sensitivity / Specificity",
                                                            plotOutput("sensSpec",
                                                                       height = "650px"),
                                                            div(strong("Figure 2.2."),"Sensitivity and specificity per period based on whether the log likehood ratio for a negative or positive control exceeded the critical value in that period or any before."))
                                                 )
                                )
                         )
                       )
              ),
              tabPanel("Database information",
                       plotOutput("databaseInfoPlot"),
                       div(strong("Figure 3.1."),"Overall distributions of key characteristics in each database."),
                       dataTableOutput("databaseInfoTable"),
                       div(strong("Table 3.2."),"Information about each database.")
              )
            )
  )
)


