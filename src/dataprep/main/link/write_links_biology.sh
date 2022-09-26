#!/bin/bash

# run as bash $script_path/link/write_links_biology from /src/dataprep

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
    
    field="biology" 
    RECALL=0.9
    train_name="${USER}_degree0"
    keywords=False
    fieldofstudy_cat=False 
    fieldofstudy_str=False 
    institution=True 
    mergemode="m:1"

    python3 -m main.link.create_link_mag_proquest --linking_type "advisors" --no-test --mergemode $mergemode --train_name $train_name \
        --field "${field}" --recall $RECALL --start 1985 --end 2022 --institution $institution \
        --loadstart 1985 --loadend 2010 \
        --to "csv" \
        --fieldofstudy_cat $fieldofstudy_cat --fieldofstudy_str $fieldofstudy_str --keywords $keywords \
        --verbose 2>&1 | tee $logfile_path/createlink_mag_proquest_"${field}"_${mergemode}_${train_name}_advisors_8510.log 
    

    # python3 -m main.link.create_link_mag_proquest --linking_type "advisors" --no-test --mergemode $mergemode --train_name $train_name \
    #     --field "${field}" --recall $RECALL --start 1985 --end 2022 --institution $institution \
    #     --loadstart 2000 --loadend 2022 \
    #     --to "csv" \
    #     --fieldofstudy_cat $fieldofstudy_cat --fieldofstudy_str $fieldofstudy_str --keywords $keywords \
    #     --verbose 2>&1 | tee $logfile_path/createlink_mag_proquest_"${field}"_${mergemode}_${train_name}_advisors_0022.log 
    


    conda deactivate
fi 
