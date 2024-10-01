#!/bin/bash

output_path="../../output"
script_path="main"
logfile_path="temp"

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

# ## Download and read in Novelty and Reuse data (from Zenodo)
bash ./main/load_novelty_reuse/download.sh &> $logfile_path/download_novelty_reuse.log
bash ./main/load_novelty_reuse/dump_db.sh &> $logfile_path/dump_db.log
rm ~/novelty/papers_textual_metrics.csv # delete the downloaded file

# ## Make some tables to speed up queries down the road 

# ### General
python3 $script_path/prep_mag/paper_fields.py &> $logfile_path/paper_fields.log

python3 $script_path/prep_mag/prep_authors.py --years_first_field 5 \
    --years_last_field 5 &> $logfile_path/prep_authors.log

python -m $script_path.prep_mag.authors_fields_detailed &> $logfile_path/authors_fields_detailed.log

python3 -m $script_path.prep_mag.prep_collab --nauthors "all" --chunksize 10000 --ncores 10 \
    &> $logfile_path/prep_collab.log
python3 -m $script_path.prep_mag.read_collab &> $logfile_path/read_collab.log

python3 $script_path/prep_mag/prep_affiliations.py &> $logfile_path/prep_affiliations.log

python3 $script_path/prep_mag/prep_citations.py &> $logfile_path/prep_citations.log

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

python3 -m $script_path.prep_mag.read_quantiles_papercites \
    quantiles_lvl1_temp quantiles_lvl0_temp \
    &> $logfile_path/read_quantiles_papercites.log

python3 $script_path/prep_mag/paper_outcomes.py &> $logfile_path/paper_outcomes.log

# TODO: add here venue_citations


python -m $script_path.prep_mag.author_field0 \
    &> $logfile_path/author_field0.log

python3 -m $script_path.prep_mag.affiliation_outcomes --fos_max_level 0 \
    &> $logfile_path/affiliation_outcomes.log #note: script_path should omit the / at the end

python -m $script_path.prep_mag.paper_language &> $logfile_path/paper_language.log


# ## Consolidate gender per author in author_sample 
python3 $script_path/prep_mag/author_gender.py &> $logfile_path/author_gender.log

# ## Tidy and correspond US affiliations
bash $script_path/institutions/clean_link_institutions.sh $logfile_path

# ## Load ProQuest data
python -m $script_path.load_proquest.proquest_to_db &> \
    $logfile_path/proquest_to_db.log

python -m $script_path.load_proquest.correspond_fieldofstudy &> \
    $logfile_path/correspond_fieldofstudy.log

# moved this down because we need the output from `clean_link_institutions` here
python3 -m $script_path.prep_mag.author_info_linking \
    --years_first_field 7 \
    &> $logfile_path/author_info_linking.log

python -m $script_path.load_proquest.pq_author_info_linking &> \
    $logfile_path/pq_author_info_linking.log

python -m $script_path.load_proquest.magfos_to_db &> \
    $logfile_path/pq_magfos_to_db.log
    
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

Rscript -e "rmarkdown::render('$script_path/reports/quality_linking.Rmd', output_dir = '$output_path')" \
    &> $logfile_path/quality_linking.log

Rscript -e "rmarkdown::render('$script_path/reports/quality_linking_graduates_chemistry.Rmd', output_dir = '$output_path')" \
    &> $logfile_path/quality_linking_graduates_chemistry.log

# ## 2. Link thesis advisors to MAG
bash $script_path/link/advisors.sh &> $logfile_path/link_advisors.log


# ## 3. Postprocessing
Rscript -e "rmarkdown::render('$script_path/reports/compare_linking.Rmd', output_dir = '$output_path')" \
    &> $logfile_path/compare_linking.log

# ### Select overlap sample from both labellers
Rscript -e "rmarkdown::render('$script_path/link/combine_links.Rmd', output_dir = '$output_path')" \
    &> $logfile_path/combine_links.log

# Manual split of biology! TODO: can we delete this?
# bash $script_path/link/write_links_biology
# python -m $script_path.link.merge_biology_csv

# ### Load into db
python -m $script_path.link.write_csv_links \
    --linking_type "graduates" \
    --train_name "combined" \
    --keepyears "19852015" \
    &> $logfile_path/write_csv_links_graduates.log

python -m $script_path.link.write_csv_links \
    --linking_type "advisors" \
    --train_name "combined" \
    &> $logfile_path/write_csv_links_advisors.log
    
Rscript -e "rmarkdown::render('$script_path/reports/quality_linking_advisors.Rmd', output_dir = '$output_path')" \
    &> $logfile_path/quality_linking_advisors.log # TODO: should this come earlier?

# ## 4. Create more variables for the linked sample

# ### Generate panel data set etc. for the linked entities
python -m $script_path.link.prep_linked_data \
    --filter_trainname_graduates "combined" \
    --filter_trainname_advisors "combined" \
    &> $logfile_path/prep_linked_data.log

# ### Calculate topic overlap between linked graduates and 
    # possible new employers & colleagues
for max_level in {2..5}; do
    python -m $script_path.link.topic_similarity \
        --top_n_authors 200 \
        --write_dir similarities_temp/ \
        --window_size 5 \
        --ncores 12 \
        --max_level $max_level \
	--parallel \
        &> "$logfile_path/topic_similarity_max_level_${max_level}.log"
done

python -m  $script_path.link.read_topic_similarity \
    --read_dir similarities_temp/ \
    &> $logfile_path/read_topic_similarity.log



# ## 5. Link NSF grants to MAG advisors
bash $script_path/link/grants.sh $logfile_path

# XXX adapt for grants - use mona train
#python -m $script_path.link.write_csv_links --linking_type "advisors" --train_name "christoph_degree0" \
#    &> $logfile_path/write_csv_links_advisors.log
    
Rscript -e "rmarkdown::render('$script_path/reports/quality_linking_grants.Rmd', output_dir = '$output_path')" \
    &> $logfile_path/quality_linking_grants.log


