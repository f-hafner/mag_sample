#!/bin/bash

logfile_path=$1
RECALL=0.9
train_name="${USER}_with_protocol_magkeywords"
keywords=True
fieldofstudy_cat=False 
fieldofstudy_str=False 
institution=False 

# "art"
# "medicine"
fields=("biology"
    "business"
    "chemistry"
    "computer science" 
    "economics"
    "engineering"
    "environmental science"
    "geography"
    "geology" 
    "history"
    "materials science"
    "mathematics"
    "philosophy"
    "physics"
    "political science"
    "psychology" 
    "sociology")



for i in "${!fields[@]}"; do 
    field=${fields[$i]} 
    echo $field
    screen -dmS "graduates.$field" sh main/link/link_onefield_graduates.sh $RECALL "$field" $train_name $institution $fieldofstudy_cat $fieldofstudy_str $keywords $logfile_path
    echo "Started screen ..."
done 

