#!/bin/bash

logfile_path=$1
RECALL=0.9
train_name="${USER}_degree0"
keywords=False
fieldofstudy_cat=False 
fieldofstudy_str=False 
institution=True

fields=("chemistry")
      #  "sociology" 
       # "mathematics"
       # "biology" 
       # "computer science" 
       # "political science"
      #  "engineering" 
      #  "psychology" 
      #  "environmental science"
      #  "physics" 
      #  "geology" 
      #  "geography"
      #  "economics")

fields=("chemistry")

for i in "${!fields[@]}"; do 
    field=${fields[$i]} 
    echo $field

   # screen -dmS "earlygrants.$field" sh main/link/link_onefield_grants.sh $RECALL "$field" $train_name $institution $fieldofstudy_cat $fieldofstudy_str $keywords $logfile_path "1980" "2009"

    screen -dmS "lategrants.$field" sh main/link/link_onefield_grants.sh $RECALL "$field" $train_name $institution $fieldofstudy_cat $fieldofstudy_str $keywords $logfile_path "2010" "2020"
done 
