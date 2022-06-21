#!/bin/bash

output_path="../../../output/"
script_path="main"
logfile_path="temp/"

python3 -m $script_path.prep_mag.prep_collab &> $logfile_path/prep_collab.log
