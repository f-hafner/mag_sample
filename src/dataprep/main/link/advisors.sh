#!/bin/bash

logfile_path=$1
RECALL=0.9
train_name="${USER}_degree0_with_protocol_updated"
keywords=False
fieldofstudy_cat=False 
fieldofstudy_str=False 
institution=True 
fields=("art"
    "biology"
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
    "medicine"
    "philosophy"
    "physics"
    "political science"
    "psychology" 
    "sociology")


for i in "${!fields[@]}"; do     
    field=${fields[$i]} 
    echo ${field}
    screen -dmS "advisors.${field}" sh main/link/link_onefield_advisors.sh $RECALL "$field" $train_name $institution $fieldofstudy_cat $fieldofstudy_str $keywords $logfile_path
    echo "Started screen ..."
done 
wait



# start_field_i()  {
#     fields=("art"
#         "biology"
#         "business"
#         "chemistry"
#         "computer science" 
#         "economics"
#         "engineering"
#         "environmental science"
#         "geography"
#         "geology" 
#         "history"
#         "materials science"
#         "mathematics"
#         "medicine"
#         "philosophy"
#         "physics"
#         "political science"
#         "psychology" 
#         "sociology")
#     field=${fields[$1]} 
#     echo $1
#     echo ${field}
#     screen -dmS "advisors.${field}" sh main/link/link_onefield_advisors.sh $RECALL "$field" $train_name $institution $fieldofstudy_cat $fieldofstudy_str $keywords $logfile_path &
#     while screen -list | grep -q $"advisors.${field}"
#     do
#         sleep 1
#     done
#     echo "Started screen ..."
#     wait 
# }
# export -f start_field_i
# parallel -j 3 start_field_i ::: $(seq 0 18)

