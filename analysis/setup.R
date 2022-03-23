
# Set up parameters for descriptives 

set.seed(1234)


## Parameters

### Common
threshold_prob_female <- 0.8
select_cohorts <- 1990
size_careerbin <- 1 # should data be aggregated across multiple years of the career? 
end_year_duration <- 2015 # duration analysis: censor all observations above end_year_duration

min_careerlength <- 5 # focus on authors with at least this career length

degree_year_start <- 1985
degree_year_end <- 2005


geemp_fields <- c("geology", "geography", "environmental science",
                  "mathematics", "computer science", "engineering",
                  "chemistry", "physics", "economics") 
lps_fields <- c("biology", "psychology", "sociology", "political science")


### For duration
probs_quantiles <- c(0.25, 0.5, 0.75)


## Packages
packages <- c("tidyverse", "broom", "dbplyr", "RSQLite", "ggplot2", 
              "lfe", "fixest", "texreg", "rlist", "oaxaca", "survival", 
              "ggfortify", "forcats", "stargazer", "stringdist")

lapply(packages, library, character.only = TRUE)


## DB 
db_file  <- "/mnt/ssd/AcademicGraph/AcademicGraph.sqlite"

# ## db connection
con <- DBI::dbConnect(RSQLite::SQLite(), db_file)
cat("The database connection is: \n")
print(src_dbi(con))
