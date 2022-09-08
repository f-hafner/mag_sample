#!/bin/bash

logfile_path=$1
RECALL=0.9
train_name="${USER}_baseline_update"
keywords=False
fieldofstudy_cat=False 
fieldofstudy_str=False 
institution=True 

fields=("physics"
        "chemistry"
        "biology" 
        "computer science" 
        "political science"
        "engineering" 
        "psychology" 
        "environmental science" 
        "geology" 
        "geography"
        "economics"
        )
#fields_done=("sociology" "mathematics")

for i in "${!fields[@]}"; do 
    field=${fields[$i]} 

    sh main/link/link_onefield_advisors.sh $RECALL "$field" $train_name $institution $fieldofstudy_cat $fieldofstudy_str $keywords $logfile_path
done 
