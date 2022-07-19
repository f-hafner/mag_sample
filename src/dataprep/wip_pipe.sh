#!/bin/bash

output_path="../../../output/"
script_path="main"
logfile_path="temp/"


python3 -m $script_path.prep_mag.author_info_linking --years_first_field 5 \
    &> $logfile_path/author_info_linking.log

# ## Link NSF grants to MAG advisors
# bash $script_path/link/grants.sh $logfile_path
