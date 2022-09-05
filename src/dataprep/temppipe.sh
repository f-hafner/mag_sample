#!/bin/bash

output_path="../../output/"
script_path="main"
logfile_path="temp/"

python3 -m $script_path.load_proquest.proquest_to_db &> $logfile_path/proquest_to_db.log

# ## Tidy and correspond US affiliations
bash $script_path/institutions/clean_link_institutions.sh $logfile_path

