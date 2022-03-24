#!/bin/bash


output_path="../output/"
logfile_path="temp/"

Rscript -e "rmarkdown::render('graduation_trends.Rmd', output_dir = '$output_path')" &> $logfile_path/graduation_trends.log
Rscript -e "rmarkdown::render('quality_current_links.Rmd', output_dir = '$output_path')" &> $logfile_path/quality_current_links.log

Rscript -e "rmarkdown::render('pub_dynamics.Rmd', output_dir = '$output_path')" &> $logfile_path/pub_dynamics.log
Rscript -e "rmarkdown::render('duration.Rmd', output_dir = '$output_path')" &> $logfile_path/duration.log
