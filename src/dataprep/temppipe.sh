#!/bin/bash

output_path="../../output/"
script_path="main"
logfile_path="temp/"


python3 -m $script_path.prep_mag.prep_quantiles_papercites \
    --nfields "all" \
    --ncores 10 \
    --write_dir "quantiles_temp" \
    &> $logfile_path/prep_quantiles_papercites.log