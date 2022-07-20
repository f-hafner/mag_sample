#!/bin/bash

output_path="../../../output/"
script_path="main"
logfile_path="temp/"


# ## Link NSF grants to MAG advisors
bash $script_path/link/grants.sh $logfile_path
