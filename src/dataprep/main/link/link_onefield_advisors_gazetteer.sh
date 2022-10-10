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

# XXX TODO: add an option on how many potential links to keep!

python3 -m main.link.train_link_gazetteer --linking_type "advisors" --no-test \
    --train_name $train_name \
    --field "${field}" --recall $RECALL --start 1990 --end 2015 --institution $institution \
    --fieldofstudy_cat $fieldofstudy_cat --fieldofstudy_str $fieldofstudy_str --keywords $keywords \
    --verbose 2>&1 | tee $logfile_path/trainlink_"${field}"_${train_name}_advisors_gazetteer_9015.log 

python3 -m main.link.create_link_gazetteer --linking_type "advisors" --no-test --train_name $train_name  \
    --field "${field}" --recall $RECALL --start 1990 --end 2015 --institution $institution \
    --to "csv" \
    --fieldofstudy_cat $fieldofstudy_cat --fieldofstudy_str $fieldofstudy_str --keywords $keywords \
    --verbose 2>&1 | tee $logfile_path/createlink_"${field}"_${train_name}_advisors_gazetteer_9015.log 
 
 