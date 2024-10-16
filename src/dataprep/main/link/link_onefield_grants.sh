#!/bin/bash


RECALL=$1
field=${2} 
train_name=$3
institution=$4
fieldofstudy_cat=$5 
fieldofstudy_str=$6
keywords=$7
logfile_path=$8
yearstart=$9
yearend=${10}
 

mergemode="m:1"

#python3 -m main.link.train_link_mag_proquest --linking_type "grants" --no-test --mergemode $mergemode --train_name $train_name \
#    --field "${field}" --recall $RECALL --start $yearstart --end $yearend --institution $institution \
#    --fieldofstudy_cat $fieldofstudy_cat --fieldofstudy_str $fieldofstudy_str --keywords $keywords \
#    --verbose 2>&1 | tee $logfile_path/trainlink_mag_grants_"${field}"_${train_name}_${yearstart}${yearend}.log 

python3 -m main.link.create_link_mag_proquest --linking_type "grants" --no-test --mergemode $mergemode --train_name $train_name \
    --field "${field}" --recall $RECALL --start $yearstart --end $yearend --institution $institution \
    --to "csv" \
    --fieldofstudy_cat $fieldofstudy_cat --fieldofstudy_str $fieldofstudy_str --keywords $keywords \
    --verbose 2>&1 | tee $logfile_path/createlink_mag_grants_"${field}"_${mergemode}_${train_name}_${yearstart}${yearend}.log 
 
