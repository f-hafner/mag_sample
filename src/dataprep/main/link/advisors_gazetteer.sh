#!/bin/bash

logfile_path=$1
RECALL=0.9
train_name="${USER}_gazetteer"
keywords=False
fieldofstudy_cat=False 
fieldofstudy_str=False 
institution=True 

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
fields=("biology")
#fields=("geology")

for i in "${!fields[@]}"; do 
    field=${fields[$i]} 
    echo ${field}
    screen -dmS "g.advisors.${field}" sh main/link/link_onefield_advisors_gazetteer.sh $RECALL "${field}" $train_name $institution $fieldofstudy_cat $fieldofstudy_str $keywords $logfile_path
    echo "Started screen ..."
done 
