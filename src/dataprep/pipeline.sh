#!/bin/bash

output_path="../../output/"
script_path="main/"
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

python3 -m $script_path.prep_mag.affiliation_outcomes &> $logfile_path/affiliation_outcomes.log #note: script_path should omit the / at the end

python3 $script_path/prep_mag/prep_citations.py &> $logfile_path/prep_citations.log

python3 $script_path/prep_mag/paper_outcomes.py &> $logfile_path/paper_outcomes.log

python3 $script_path/prep_mag/author_info_linking.py --years_first_field 5 \
    &> $logfile_path/author_info_linking.log

# ## Consolidate gender per author in author_sample 
python3 $script_path/prep_mag/author_gender.py &> $logfile_path/author_gender.log

# ## Load ProQuest data
python3 $script_path/load_proquest/proquest_to_db.py &> $logfile_path/proquest_to_db.log
python3 $script_path/load_proquest/correspond_fieldofstudy.py &> $logfile_path/correspond_fieldofstudy.log

# ## Make some summary stats on the data
Rscript $script_path/reports/data_nces.R &> temp/data_nces.log  

Rscript -e "rmarkdown::render('$script_path/reports/quality_mag.Rmd', output_dir = '$output_path')"  \
    &> $logfile_path/quality_mag.log

Rscript -e "rmarkdown::render('$script_path/reports/quality_proquest.Rmd', output_dir = '$output_path')" \
    &> $logfile_path/quality_proquest.log


# ## Link graduates
bash $script_path/link/graduates.sh $logfile_path

Rscript -e "rmarkdown::render('$script_path/reports/quality_linking.Rmd', output_dir = '$output_path')" \
    &> $logfile_path/quality_linking.log

python3 $script_path/link/prep_linked_data.py &> $logfile_path/prep_linked_data.log

# ## Link linked graduates to supervisory activity. 
bash $script_path/link/advisors.sh $logfile_path

Rscript -e "rmarkdown::render('$script_path/reports/advisor_links_quality_select.Rmd', output_dir = '$output_path')" \
    &> $logfile_path/advisor_links_quality_select.log

