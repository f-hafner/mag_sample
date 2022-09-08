#!/bin/bash

logfile_path=$1
RECALL=0.9
train_name="${USER}_baseline_v2"
keywords=False
fieldofstudy_cat=False 
fieldofstudy_str=False 
institution=True

fields=("geology" "geography" "chemistry"
        "sociology" "mathematics"
        "biology" "computer science" "political science"
        "engineering" "psychology" "environmental science"
        "physics" "economics")

for i in "${!fields[@]}"; do 
    field=${fields[$i]} 

    sh main/link/link_onefield_grants.sh $RECALL "$field" $train_name $institution $fieldofstudy_cat $fieldofstudy_str $keywords $logfile_path
    # TODO: need dynamic path here 
done 
