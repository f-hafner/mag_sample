#!/bin/bash

output_path="../../output/"
script_path="main"
logfile_path="temp/"

python3 -m $script_path.prep_mag.prep_quantiles_papercites \
    --nfields "all" \
    --ncores 10 \
    --write_dir "quantiles_lvl0_temp" \
    --level 0 \
    &> $logfile_path/prep_quantiles_papercites_lvl0.log

python3 -m $script_path.prep_mag.prep_quantiles_papercites \
    --nfields "all" \
    --ncores 10 \
    --write_dir "quantiles_lvl1_temp" \
    --level 1 \
    &> $logfile_path/prep_quantiles_papercites_lvl1.log

# python3 -m $script_path.prep_mag.read_quantiles_papercites \
#     --read_dir "quantiles_temp" \
#     &> $logfile_path/read_quantiles_papercites.log
