#!/bin/bash

logfile_path=$1
RECALL=0.9
train_name="${USER}_degree0"
keywords=True
fieldofstudy_cat=False 
fieldofstudy_str=False 
institution=False 

fields=("chemistry"
        "sociology" 
        "mathematics"
        "biology" 
        "computer science" 
        "political science"
        "engineering" 
        "psychology" 
        "environmental science"
        "physics" 
        "geology" 
        "geography"
        "economics")
        
fields=("art"
        "business"
        "history"
        "materials science"
        "medicine"
        "philosophy"
         )
 fields=("philosophy"
         )
 

for i in "${!fields[@]}"; do 
    field=${fields[$i]} 
    echo $field
    screen -dmS "graduates.$field" sh main/link/link_onefield_graduates.sh $RECALL "$field" $train_name $institution $fieldofstudy_cat $fieldofstudy_str $keywords $logfile_path
    echo "Started screen ..."
done 

