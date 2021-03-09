printConceptSet <- function(conceptSet, latex_table_font_size) {
  
  markdown <- CirceR::conceptSetPrintFriendly(conceptSet)
  rows <- unlist(strsplit(markdown, "\\r\\n"))
  rows <- gsub("^\\|", "", rows)
  header <- rows[1]
  data <- readr::read_delim(paste(rows[c(2,4:(length(rows)-2))], 
                                  collapse = '\n'), delim = '|',)
  
  header <- gsub("###", "### Concept set:", header)
  
  tab <- data %>% 
    mutate_if(is.numeric, format, digits = 10) %>% 
    kable(linesep = "", booktabs = TRUE, longtable = TRUE)
  
  if (knitr::is_latex_output()) {    
    writeLines(header)
    
    writeLines(tab %>% 
      kable_styling(latex_options = "striped", font_size = latex_table_font_size) %>%
      column_spec(1, width = "5em") %>%
      column_spec(2, width = "22em") %>%
      column_spec(3, width = "5em") %>%
      column_spec(4, width = "5em") %>%
      column_spec(5, width = "4em") %>%
      column_spec(6, width = "5em") %>%
      column_spec(7, width = "3em"))
  } else if (knitr::is_html_output()) {
    writeLines(header)
    
    writeLines(tab %>% 
                 kable_styling(bootstrap_options = "striped"))
  } else {
    writeLines(markdown)
  }
}

printCohortClose <- function() {
  writeLines("")
  if (knitr::is_html_output()) {
    writeLines("<hr style=\"border:2px solid gray\"> </hr>")
  } else {
    writeLines("------")
  }
  writeLines("")
}

printCohortDefinitionFromNameAndJson <- function(name, json = NULL, obj = NULL, 
                                                 withConcepts = TRUE,
                                                 withClosing = TRUE, latex_table_font_size = 8) {
  
  if (is.null(obj)) {
    obj <- CirceR::cohortExpressionFromJson(json)
  }
  
  writeLines(paste("##", name, "\n"))
  
  # Print main definition
  markdown <- CirceR::cohortPrintFriendly(obj)
  
  markdown <- gsub("criteria:\\r\\n ", "criteria:\\\r\\\n\\\r\\\n ", markdown)
  markdown <- gsub("old.\\r\\n\\r\\n", "old.\\\r\\\n", markdown)
  
  markdown <- gsub("The person exits the cohort", "\\\r\\\nThe person also exists the cohort", markdown)
  markdown <- gsub("following events:", "following events:\\\r\\\n", markdown)
  
  rows <- unlist(strsplit(markdown, "\\r\\n")) 
  rows <- gsub("^   ", "", rows)
  markdown <- paste(rows, collapse = "\n")
  
  writeLines(markdown)
  
  # Print concept sets
  
  if (withConcepts) {
    lapply(obj$conceptSets, printConceptSet, latex_table_font_size = latex_table_font_size)
  }
  
  if (withClosing) {
    printCohortClose()
  }
}

printCohortDefinition <- function(info) {
  json <- SqlRender::readSql(info$jsonFileName)
  printCohortDefinitionFromNameAndJson(info$name, json)
}

printInclusionCriteria <- function(obj, removeDescription = FALSE) {
  
  markdown <- CirceR::cohortPrintFriendly(obj)
  markdown <- sub(".*### Inclusion Criteria", "", markdown)
  markdown <- sub("### Cohort Exit.*", "", markdown)
  markdown <- gsub("### \\d+.", "##", markdown)
  markdown <- gsub("criteria:\\r\\n ", "criteria:\\\r\\\n\\\r\\\n ", markdown)
  
  rows <- unlist(strsplit(markdown, "\\r\\n")) 
  rows <- gsub("^   ", "", rows)
  markdown <- paste(rows, collapse = "\n")
  
  writeLines(markdown)
}

printExitCriteria <- function(obj) {
  
  markdown <- CirceR::cohortPrintFriendly(obj)
  markdown <- sub(".*### Cohort Exit", "", markdown)
  markdown <- sub("### Cohort Eras.*", "", markdown)
  markdown <- sub("The person exits the cohort", "\\\r\\\nThe person also exists the cohort", markdown)
  markdown <- sub("following events:", "following events:\\\r\\\n", markdown)
  
  writeLines(markdown)
}