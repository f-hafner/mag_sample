#!/bin/bash

# TODO: the directory will still be src/main, so this will not work at the moment

RECALL=$1
field=${2} 
train_name=$3
institution=$4
fieldofstudy_cat=$5 
fieldofstudy_str=$6
keywords=$7
logfile_path=$8
echo "$field"

mergemode="m:1"

python3 train_link_mag_proquest.py --linking_type "advisors" --no-test --train_name $train_name \
    --field "${field}" --recall $RECALL --start 1985 --end 2022 --institution $institution \
    --fieldofstudy_cat $fieldofstudy_cat --fieldofstudy_str $fieldofstudy_str --keywords $keywords \
    --verbose 2>&1 | tee $logfile_path/trainlink_mag_proquest_"${field}"_${train_name}_advisors_8522.log 

python3 create_link_mag_proquest.py --linking_type "advisors" --no-test --mergemode $mergemode --train_name $train_name \
    --field "${field}" --recall $RECALL --start 1985 --end 2022 --institution $institution \
    --fieldofstudy_cat $fieldofstudy_cat --fieldofstudy_str $fieldofstudy_str --keywords $keywords \
    --verbose 2>&1 | tee $logfile_path/createlink_mag_proquest_"${field}"_${mergemode}_${train_name}_advisors_8522.log 
 
