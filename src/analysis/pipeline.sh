#!/bin/bash

# Change to the directory containing this script
cd "$(dirname "$0")"

output_path="../../output/"
logfile_path="temp/"
rprofile_path="../../.Rprofile"

# Function to run Rscript with renv activation
run_rscript() {
    Rscript --vanilla -e "setwd('../../'); source('.Rprofile'); renv::load(); setwd('src/analysis'); $1"
}

run_rscript "rmarkdown::render('graduation_trends.Rmd', output_dir = '$output_path')" \
    &> $logfile_path/graduation_trends.log
run_rscript "rmarkdown::render('quality_current_links.Rmd', output_dir = '$output_path')" \
    &> $logfile_path/quality_current_links.log

run_rscript "rmarkdown::render('pub_dynamics.Rmd', output_dir = '$output_path')" \
    &> $logfile_path/pub_dynamics.log
run_rscript "rmarkdown::render('duration.Rmd', output_dir = '$output_path')" &> \
    $logfile_path/duration.log
