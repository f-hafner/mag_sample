#!/bin/bash

## Information
#--------------
# Script to label/link records from MAG and NSF.
# Some housekeeping is necessary b/c of an unresolved issue with dedupe>2.0.11
# Usage:
    # Install science-career-tempenv.yml according to instructions (necessary only once)
    # Run this script

# 1. Local variables 
## for deduping
output_path="../../../output/"
script_path="main"
logfile_path="temp/"
## for conda 
BASEENV='base'
NECESSARYENV='science-career-tempenv'
NECESSARYVERSION="2.0.11"

# ## 2. Activate the right conda environment for deduping
eval "$(conda shell.bash hook)"

ENV=$(conda info | grep "active environment" | awk '{ gsub("\t active environment : ", ""); print}')
if ! grep -q "$BASEENV" <<< "$ENV"; then
  echo "Base environment not activated."
  exit 
fi

DEDUPEVERSION=$(conda list -n science-career-tempenv | grep 'dedupe ')
if ! grep -q "$NECESSARYVERSION" <<< $DEDUPEVERSION; then 
  echo "Wrong version of dedupe in environment science-career-tempenv."
  conda deactivate
else
    conda activate $NECESSARYENV
    # ## 2. Link NSF grants to MAG advisors
    echo "Starting main script"
    bash $script_path/link/grants.sh $logfile_path
    # ## 3. Finish
    conda deactivate
fi 
