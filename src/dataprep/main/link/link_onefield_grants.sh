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

pwd 

mergemode="m:1"
mkdir "~/tmpgrants_${field}/"
export TMP="~/tmpgrants_${field}"

python3 -m main.link.train_link_mag_proquest --linking_type "grants" --no-test --mergemode $mergemode --train_name $train_name \
    --field "${field}" --recall $RECALL --start 1980 --end 2020 --institution $institution \
    --fieldofstudy_cat $fieldofstudy_cat --fieldofstudy_str $fieldofstudy_str --keywords $keywords \
    --verbose 2>&1 | tee $logfile_path/trainlink_mag_grants_"${field}"_${train_name}_8020.log 

rm -rf ~/tmpgrants_${field}/*

python3 -m main.link.create_link_mag_proquest --linking_type "grants" --no-test --mergemode $mergemode --train_name $train_name \
    --field "${field}" --recall $RECALL --start 1980 --end 2020 --institution $institution \
    --to "csv" \
    --fieldofstudy_cat $fieldofstudy_cat --fieldofstudy_str $fieldofstudy_str --keywords $keywords \
    --verbose 2>&1 | tee $logfile_path/createlink_mag_grants_"${field}"_${mergemode}_${train_name}_8020.log 
 
rm -rf ~/tmpgrants_${field}
