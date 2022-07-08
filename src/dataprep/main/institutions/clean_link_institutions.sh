#!/bin/bash

logfile_path=$1

echo "loading cng..."
python -m main.institutions.cng_to_db 2>&1 | tee $logfile_path/cng_to_db.log

echo "linking mag to cng..."
python -m main.institutions.link_cng_mag 2>&1 | tee $logfile_path/link_cng_mag.log

# overview_links_cng_mag.ipyng -- how can I convert this to pdf?