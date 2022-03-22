#!/bin/bash

RECALL=$1
field=${2} 
train_name=$3
institution=$4
fieldofstudy_cat=$5 
fieldofstudy_str=$6
keywords=$7
echo "$field"

python3 train_link_mag_proquest.py --no-test --train_name $train_name --field "${field}" --recall $RECALL --start 1985 --end 2005 --institution $institution --fieldofstudy_cat $fieldofstudy_cat --fieldofstudy_str $fieldofstudy_str --keywords $keywords --verbose 2>&1 | tee temp/trainlink_mag_proquest_"${field}"_${train_name}_8505.log 
 
mergemode="1:1"
python3 create_link_mag_proquest.py --no-test --mergemode $mergemode --train_name $train_name --field "${field}" --recall $RECALL --start 1985 --end 2005 --institution $institution --fieldofstudy_cat $fieldofstudy_cat --fieldofstudy_str $fieldofstudy_str --keywords $keywords --verbose 2>&1 | tee temp/createlink_mag_proquest_"${field}"_${mergemode}_${train_name}_8505.log 
 
# mergemode="m:1"
# python3 create_link_mag_proquest.py --no-test --mergemode $mergemode --train_name $train_name --field "${field}" --recall $RECALL --start 1985 --end 2005 --institution $institution --fieldofstudy_cat $fieldofstudy_cat --fieldofstudy_str $fieldofstudy_str --keywords $keywords --verbose 2>&1 | tee temp/createlink_mag_proquest_${field}_${mergemode}_${train_name}_8505.log 
 