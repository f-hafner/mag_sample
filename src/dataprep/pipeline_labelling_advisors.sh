#!/bin/bash


## for calling script
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
    # ## 2. Link Proquest advisors to MAG Authors
    echo "Starting main script"
    bash $script_path/link/advisors.sh $logfile_path
    # ## 3. Finish
    conda deactivate
fi 
