#!/bin/bash

output_path="../../output/"
script_path="main"
logfile_path="temp/"


python -m $script_path.link.write_csv_links --linking_type "advisors" --train_name "christoph_degree0" \
    &> $logfile_path/write_csv_links_advisors.log

Rscript -e "rmarkdown::render('$script_path/reports/quality_linking_advisors.Rmd', output_dir = '$output_path')" \
    &> $logfile_path/quality_linking_advisors.log

