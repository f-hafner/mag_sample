#!/bin/bash

output_path="../../output/"
script_path="main"
logfile_path="temp/"

# ## Link advisors  
#bash $script_path/link/advisors.sh &> $logfile_path/link_advisors.log

#python -m $script_path.link.prep_linked_data &> $logfile_path/prep_linked_data.log


Rscript -e "rmarkdown::render('$script_path/reports/advisor_links_quality_select.Rmd', output_dir = '$output_path')" \
    &> $logfile_path/advisor_links_quality_select.log

