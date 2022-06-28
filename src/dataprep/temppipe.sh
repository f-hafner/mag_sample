#!/bin/bash

output_path="../../../output/"
script_path="main"
logfile_path="temp/"

python3 -m $script_path.prep_mag.prep_collab --nauthors 100  &> $logfile_path/prep_collab.log
python3 -m $script_path.prep_mag.read_collab &> $logfile_path/read_collab.log


# python3 -m main.prep_mag.prep_collab --nauthors 100
# python3 -m main.prep_mag.read_collab 