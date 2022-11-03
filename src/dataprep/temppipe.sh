#!/bin/bash

output_path="../../output/"
script_path="main"
logfile_path="temp/"

Rscript -e "rmarkdown::render('$script_path/reports/quality_linking_advisors.Rmd', output_dir = '$output_path')" \
    &> $logfile_path/quality_linking_advisors.log