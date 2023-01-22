#!/bin/bash

output_path="../../output/"
script_path="main"
logfile_path="temp/"


python3 -m $script_path.prep_mag.prep_quantiles_papercites \
    --nfields 1 \
    --ncores 10 \
    --end_year 1980 \
    --write_dir "quantiles_temp" \
    &> $logfile_path/prep_quantiles_papercites.log