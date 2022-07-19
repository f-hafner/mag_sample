#!/bin/bash


RECALL=$1
field=${2} 
train_name=$3
institution=$4
fieldofstudy_cat=$5 
fieldofstudy_str=$6
keywords=$7
logfile_path=$8
echo "$field"

mergemode="1:1"

pwd

python3 -m main.link.train_link_mag_proquest --linking_type "grants" --test --train_name $train_name \
    --field "${field}" --recall $RECALL --start 1985 --end 2022 --institution $institution \
    --fieldofstudy_cat $fieldofstudy_cat --fieldofstudy_str $fieldofstudy_str --keywords $keywords \
    --verbose 2>&1 | tee $logfile_path/trainlink_mag_proquest_"${field}"_${train_name}_grants_8522.log 

python3 -m main.link.create_link_mag_proquest --linking_type "grants" --test --mergemode $mergemode --train_name $train_name \
    --field "${field}" --recall $RECALL --start 1985 --end 2022 --institution $institution \
    --fieldofstudy_cat $fieldofstudy_cat --fieldofstudy_str $fieldofstudy_str --keywords $keywords \
    --verbose 2>&1 | tee $logfile_path/createlink_mag_proquest_"${field}"_${mergemode}_${train_name}_grants_8522.log 
 
