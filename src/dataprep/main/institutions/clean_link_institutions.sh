#!/bin/bash

# Create correspondence from MAG and ProQuest to 
# Carnegie classification.

# Purpose:
    # 1. Link institutions across data sets 
    # 2. Define a set of academic institutions in MAG for analysis
# See the file overview_links.ipynb in this directory 
    # for some to dos and an overview of the links.

logfile_path=$1

rawdatapath="../../data/institutions" # raw cng and zip code data
tempdatapath="../../data/link_institutions/" # store temporary csv file
cng_file="CCIHE2021-PublicData.xlsx"
zip_code_file="ZIP_codes_2020.xls"

link_zip="https://mcdc.missouri.edu/applications/zipcodes/ZIP_codes_2020.xls"
link_cng="https://carnegieclassifications.acenet.edu/downloads/CCIHE2021-PublicData.xlsx"

min_score_mag=0.6
min_score_pq=0.3
file_pq="links_pq.csv"
file_mag="links_mag.csv"

if [ ! -d "$tempdatapath" ]; then 
    echo "making new dir for tempdata"
    mkdir $tempdatapath
fi

# 0. download the original data files
wget -O $rawdatapath/$zip_code_file $link_zip
wget -O $rawdatapath/$cng_file $link_cng 

# 1. prep carnegie data, link
echo "loading cng..."
python -m main.institutions.cng_to_db \
    --rawdata $rawdatapath \
    --cng $cng_file \
    --zipcodes $zip_code_file \
    2>&1 | tee $logfile_path/cng_to_db.log

echo "linking mag to cng..."
python -m main.institutions.link_cng_mag \
    --tofile $tempdatapath/$file_mag \
    2>&1 | tee $logfile_path/link_cng_mag.log

echo "linking pq to cng..."
python -m main.institutions.link_cng_pq \
    --tofile $tempdatapath/$file_pq \
    --maglinks $tempdatapath/$file_mag \
    --minmag $min_score_mag
    2>&1 | tee $logfile_path/link_cng_pq.log

# # 2. run the notebook overview_links.ipynb 
    # interactively to see a summary of the work above 

# 3. Save links with min_score to db
echo "writing accepted links to db..."
python -m main.institutions.cng_links_to_db \
    --fromdir $tempdatapath \
    --minmag $min_score_mag \
    --minpq $min_score_pq
    &> $logfile_path/cng_links_to_db.log

rm -rf $tempdatapath