#!/bin/bash


## for calling script
output_path="../../../output/"
script_path="main"
logfile_path="temp/"

echo "Starting main script"
bash $script_path/link/advisors.sh $logfile_path
