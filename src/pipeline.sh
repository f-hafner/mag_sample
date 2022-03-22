#!/bin/bash

# ## Read in database
python3 load_mag/create_database.py &> temp/create_database.log
    # to test: add `--nlines XX` to only read XX lines from each file
python3 load_mag/check_database.py &> temp/check_database.log

# ## Genderize 
python3 gender/genderize.py --outfile "gender_names.csv"  &> temp/genderize.log # add the genderize API key with `--apikey "mykey"` 
python3 gender/read_gendertable.py --from_file "gender_names.csv" --to_table "FirstNamesGender" --index "idx_fng_FirstName" &> temp/read_gendertable.log
python3 gender/split_unclear_names.py &> temp/split_unclear_names.log
# python3 genderize_unclear_names.py --outfile "gender_unclear_names.csv" &> temp/genderize_unclear_names.log  # add the genderize API key with `--apikey "mykey"` 
python3 gender/read_gendertable.py --from_file "gender_unclear_names.csv" --to_table "UnclearNamesGender" --index "idx_ung_FirstName" &> temp/read_gendertable_unclearnames.log

# ## Make some tables to speed up queries down the road 

# ### General
python3 prep_mag/paper_fields.py &> temp/paper_fields.log
python3 prep_mag/prep_authors.py --years_first_field 5 --years_last_field 5 &> temp/prep_authors.log
python3 prep_mag/prep_affiliations.py &> temp/prep_affiliations.log
python3 prep_mag/prep_citations.py &> temp/prep_citations.log
python3 prep_mag/paper_outcomes.py &> temp/paper_outcomes.log
python3 prep_mag/author_info_linking.py --years_first_field 5 &> temp/author_info_linking.log

# ## Consolidate gender per author in author_sample 
python3 prep_mag/author_gender.py &> temp/author_gender.log

# ## Load ProQuest data
python3 load_proquest/proquest_to_db.py &> temp/proquest_to_db.log
python3 load_proquest/correspond_fieldofstudy.py &> temp/correspond_fieldofstudy.log

# ## Make some summary stats on the data
Rscript reports/data_nces.R &> temp/data_nces.log  
Rscript -e "rmarkdown::render('reports/quality_mag.Rmd', output_dir = '../output/')" &> temp/quality_mag.log
Rscript -e "rmarkdown::render('reports/quality_proquest.Rmd', output_dir = '../output/')" &> temp/quality_proquest.log


# ## Link graduates
bash link/graduates.sh 

Rscript -e "rmarkdown::render('reports/quality_linking.Rmd', output_dir = '../output/')" &> temp/quality_linking.log

# TODO: add the prep_linked_data here instead of the researcher_gender? then we would have a single flow here. now it is not connected

# ## Link linked graduates to supervisory activity. 
bash link/advisors.sh

Rscript -e "rmarkdown::render('reports/advisor_links_quality_select.Rmd', output_dir = '../output/')" &> temp/advisor_links_quality_select.log





