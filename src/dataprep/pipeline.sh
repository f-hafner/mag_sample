#!/bin/bash

output_path="../../output/"
script_path="main"
logfile_path="temp/"

# ## Read in database
python3 $script_path/load_mag/create_database.py &> $logfile_path/create_database.log
    # add `--nlines XX` to only read XX lines from each file
python3 $script_path/load_mag/check_database.py &> $logfile_path/check_database.log

# ## Genderize 
python3 $script_path/gender/genderize.py --outfile "gender_names.csv" \
    &> $logfile_path/genderize.log # add the genderize API key with `--apikey "mykey"` 

python3 $script_path/gender/read_gendertable.py --from_file "gender_names.csv" \
    --to_table "FirstNamesGender" --index "idx_fng_FirstName" &> $logfile_path/read_gendertable.log

python3 $script_path/gender/split_unclear_names.py &> $logfile_path/split_unclear_names.log

python3 $script_path/gender/genderize_unclear_names.py --outfile "gender_unclear_names.csv" \
    &> $logfile_path/genderize_unclear_names.log  # add the genderize API key with `--apikey "mykey"` 

python3 $script_path/gender/read_gendertable.py --from_file "gender_unclear_names.csv" \
    --to_table "UnclearNamesGender" --index "idx_ung_FirstName" &> $logfile_path/read_gendertable_unclearnames.log

# ## Make some tables to speed up queries down the road 

# ### General
python3 $script_path/prep_mag/paper_fields.py &> $logfile_path/paper_fields.log

python3 $script_path/prep_mag/prep_authors.py --years_first_field 5 \
    --years_last_field 5 &> $logfile_path/prep_authors.log

python3 -m $script_path.prep_mag.prep_collab --nauthors "all" --chunksize 10000 --ncores 10 \
    &> $logfile_path/prep_collab.log
python3 -m $script_path.prep_mag.read_collab &> $logfile_path/read_collab.log

python3 $script_path/prep_mag/prep_affiliations.py &> $logfile_path/prep_affiliations.log

python3 $script_path/prep_mag/prep_citations.py &> $logfile_path/prep_citations.log

python3 $script_path/prep_mag/paper_outcomes.py &> $logfile_path/paper_outcomes.log

python3 $script_path/prep_mag/author_info_linking.py --years_first_field 7 \
    &> $logfile_path/author_info_linking.log

python -m $script_path.prep_mag.author_field0 \
    &> $logfile_path/author_field0.log

python3 -m $script_path.prep_mag.affiliation_outcomes --fos_max_level 2 \ 
    &> $logfile_path/affiliation_outcomes.log #note: script_path should omit the / at the end


# ## Consolidate gender per author in author_sample 
python3 $script_path/prep_mag/author_gender.py &> $logfile_path/author_gender.log

# ## Tidy and correspond US affiliations
bash $script_path/institutions/clean_link_institutions.sh $logfile_path

# ## Load ProQuest data
python -m $script_path.load_proquest.proquest_to_db &> \
    $logfile_path/proquest_to_db.log

python -m $script_path.load_proquest.correspond_fieldofstudy &> \
    $logfile_path/correspond_fieldofstudy.log

python -m $script_path.load_proquest.pq_author_info_linking &> \
    $logfile_path/pq_author_info_linking.log

# ## Make some summary stats on the data
Rscript $script_path/reports/data_nces.R &> temp/data_nces.log  

Rscript -e "rmarkdown::render('$script_path/reports/quality_mag.Rmd', output_dir = '$output_path')"  \
    &> $logfile_path/quality_mag.log

Rscript -e "rmarkdown::render('$script_path/reports/quality_proquest.Rmd', output_dir = '$output_path')" \
    &> $logfile_path/quality_proquest.log

Rscript -e "rmarkdown::render('$script_path/reports/sample_size_linking.Rmd', output_dir = '$output_path')" \
    &> $logfile_path/sample_size_linking.log

# # Link

# ## 1. Link graduates to MAG
bash $script_path/link/graduates.sh $logfile_path

# Christoph retrained with with the following options:
# --train_name "christoph_degree0" --keepyears "19852015"
# need to run the write_csv_links script with these options as well
# to get all links into db
python -m $script_path.link.write_csv_links --linking_type "graduates" --train_name "christoph_fielddegree0" \
    &> $logfile_path/write_csv_links_graduates.log

Rscript -e "rmarkdown::render('$script_path/reports/quality_linking.Rmd', output_dir = '$output_path')" \
    &> $logfile_path/quality_linking.log

# ## 2. Link thesis advisors to MAG
bash $script_path/link/advisors.sh &> $logfile_path/link_advisors.log

# Manual split of biology! TODO: can we delete this?
# bash $script_path/link/write_links_biology
# python -m $script_path.link.merge_biology_csv

python -m $script_path.link.write_csv_links --linking_type "advisors" --train_name "christoph_degree0" \
    &> $logfile_path/write_csv_links_advisors.log
    
Rscript -e "rmarkdown::render('$script_path/reports/quality_linking_advisors.Rmd', output_dir = '$output_path')" \
    &> $logfile_path/quality_linking_advisors.log


# ## 3. Link NSF grants to MAG advisors
bash $script_path/link/grants.sh $logfile_path


# # Generate panel data set etc. for the linked entities
python -m $script_path.link.prep_linked_data \
    --filter_trainname "christoph_" \
    &> $logfile_path/prep_linked_data.log