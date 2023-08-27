"""
Step 1 to Link MAG authors to ProQuest graduates:
         Active labelling
"""

from main.link.setup_linking import *


print("finished setup ... ", flush=True)
print_elapsed_time(start_time)

# ## Link
if __name__ == "__main__":
    settings_file = "settings"
    training_file = "training"

    if args.testing:
        settings_file = "settings_test"
        training_file = "training_test"

    # s_years = f"{args.startyear}_{args.endyear}"
    fld = field_to_store.replace(" ", "_")
    settings_file = f"{path_dedupe_files}{settings_file}_{fld}_{args.startyear}_{args.endyear}_institution{args.institution}_fieldofstudy_cat{args.fieldofstudy_cat}_fieldofstudy_str{args.fieldofstudy_str}_keywords{args.keywords}{args.train_name}"
    
    training_file = f"{path_dedupe_files}{training_file}_{fld}_{args.startyear}_{args.endyear}_institution{args.institution}_fieldofstudy_cat{args.fieldofstudy_cat}_fieldofstudy_str{args.fieldofstudy_str}_keywords{args.keywords}{args.train_name}.json" 

    # ## Load data 
    if args.linking_type == "grants":
        query_other = query_nsf
    else:
        query_other = query_proquest

    if args.testing:
        line_limit = 500
        query_other = f"{query_other} LIMIT {line_limit}"
        query_mag = f"{query_mag} LIMIT {line_limit}"

    for q in [query_other, query_mag]:
        print(f"{q} \n", flush=True)

    # https://stackoverflow.com/questions/3300464/how-can-i-get-dict-from-sqlite-query
    # https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.row_factory

    with read_dict_con as con:
        cur = con.cursor()
        if args.linking_type == "graduates": # TODO: fix this when all queries are fixed 
            cur.execute(query_mag, tuple(id_field + id_field)) # this is necessary because we query fieldofstudytable 2x
        else: 
            cur.execute(query_mag, tuple(id_field + id_field))
        magdata = {i: row for i, row in custom_enumerate(cur.fetchall(), "AuthorId")}
        cur = con.cursor()
        if args.linking_type == "grants":
            cur.execute(query_other)
            otherdata = {i: row for i, row in custom_enumerate(cur.fetchall(), nsf_entity_id)}
        else:
            cur.execute(query_other, tuple(id_field))
            otherdata = {i: row for i, row in custom_enumerate(cur.fetchall(), pq_entity_id)} # TODO: rename proquestdata to otherdata


    # transform the strings to hashable sequences
    for data in [magdata, otherdata]:
        for key in data.keys():
            if data[key]["keywords"] is not None:
                data[key]["keywords"] = frozenset([x for x in data[key]["keywords"].split(";") if x != ""]) 
                # the list comp above deals with cases "word1;word2;" (the last ; gives an empty last element in the output of split())

            features = ["institution", "coauthors", "year_range",
                        "main_us_institutions_year", "all_us_institutions_year",
                        "year_papertitle"]
            ft_in_data = list(data[list(data.keys())[0]].keys()) # extract all features of the first record in the dict data
            features = [f for f in features if f in ft_in_data]
            for feature in features:
                if data[key][feature] is not None:
                    if feature in ["main_us_institutions_year", "all_us_institutions_year",
                                    "year_papertitle"]:
                        # split, make first entry numeric, convert to tuple
                        ft = [x.split("//") for x in data[key][feature].split(";")]
                        ft = [tuple([int(x[0]), x[1]]) for x in ft] 
                        data[key][feature] = tuple(ft)
                    elif feature == "year_range":
                        ft = data[key][feature]
                        if isinstance(ft, str):
                            ft = ft.split(";")
                            ft = tuple([int(f) for f in ft])
                        else:
                            assert isinstance(ft, int) 
                            ft = (ft, )
                        data[key][feature] = ft
                    else:
                        data[key][feature] = tuple(data[key][feature].split(";"))
    
    # NOTE
        # need `frozenset` for the set feature; while the documentation says tuples also work, there is a bug 
        # in dedupe for reading tuples from training data
        # (frozensets are encoded in https://github.com/dedupeio/dedupe/blob/e010ba1790b4b9744a74b32a5f762f8eac41f74f/dedupe/serializer.py#L25,
        # but not tuples). Kept tuples for institution and coauthors as it seems more readable for labelling
    #pdb.set_trace()
    n_match = None # this will create null in database when settings were read from settings_file
    n_distinct = None
    
    if os.path.exists(settings_file):
        print(f'reading from {settings_file}', flush=True)
        with open(settings_file, 'rb') as sf:
            linker = dedupe.StaticRecordLink(sf, num_cores = n_cores)
    else:
        # define fields for categorical 
        # TODO: this is not necessary anymore b/c we do not use categorical for linking
        # if args.linking_type != "grants":
        #     query_fields_mag = f"SELECT DISTINCT(fieldofstudy) FROM ( {query_mag} ) WHERE fieldofstudy IS NOT NULL"
        #     query_fields_proquest = f"SELECT DISTINCT(fieldofstudy) FROM ( {query_other} ) WHERE fieldofstudy IS NOT NULL"
        #     mag_areas = [i[0] for i in read_con.execute(query_fields_mag, tuple(id_field)).fetchall()] 
        #     proquest_areas = [i[0] for i in read_con.execute(query_fields_proquest, tuple(id_field)).fetchall()] 
        #     areas = mag_areas + proquest_areas

        if args.linking_type == "graduates":
            # TODO: these definitions here should be eventually standardized across cases
            fields = [
                {"field": "firstname", "variable name": "firstname", "type": "String", "has missing": False},
                {"field": "firstname", "variable name": "same_firstname", "type": "Exact"},
                {"field": "lastname", "variable name": "lastname", "type": "String", "has missing": False},
                {"field": "lastname", "variable name": "same_lastname", "type": "Exact"},
                {"field": "middlename", "variable name": "middlename", "type": "String", "has missing": True},
                # {"field": "middle_lastname", "variable name": "same_name", "type": "Custom", "comparator": name_comparator},
                {"field": "year", "variable name": "year", "type": "Price"},
                # {"field": "year", "variable name": "no_advisor_info", "type": "Custom", "comparator": year_dummy_noadvisor},
                # {"field": "year", "variable name": "yeardiff_sqrd", "type": "Custom", "comparator": squared_diff},
                {"type": "Interaction", "interaction variables": ["year", "firstname"]},
                {"type": "Interaction", "interaction variables": ["year", "lastname"]},
                {"field": "year_papertitle", "variable name": "title_similarity", "type": "Custom", "comparator" : cf.year_title_comparator, "has missing": True},
                {"type": "Interaction", "interaction variables": ["title_similarity", "firstname"]},
                {"type": "Interaction", "interaction variables": ["title_similarity", "lastname"]},
                {"type": "Interaction", "interaction variables": ["title_similarity", "year"]}
                # {"field": "coauthors", "variable name": "coauthors", "type": "Custom", "comparator": max_set_similarity, "has missing": True}, 
                # {"type": "Interaction", "interaction variables": ["no_advisor_info", "coauthors"], "has missing": True},
            ]
        elif args.linking_type == "advisors":
            fields = [
                {"field": "firstname", "variable name": "firstname", "type": "String", "has missing": False},
                {"field": "lastname", "variable name": "lastname", "type": "String", "has missing": False},
                {"field": "middlename", "variable name": "middlename", "type": "String", "has missing": True},
                {"field": "year_range", "variable name": "year_range", "type": "Custom", "comparator": cf.compare_range_from_tuple_tempfix, "has missing": True},
                {"field": "year_range", "variable name": "year_range", "type": "Custom", "comparator": cf.compare_startrange_from_tuple, "has missing": True},
                {"field": "year_range", "variable name": "year_range", "type": "Custom", "comparator": cf.compare_endrange_from_tuple, "has missing": True}
            ] 
        elif args.linking_type == "grants":
            fields = [
                {"field": "firstname", "variable name": "firstname", "type": "String", "has missing": False},
                {"field": "lastname", "variable name": "lastname", "type": "String", "has missing": False},
                {"field": "middlename", "variable name": "middlename", "type": "String", "has missing": True},
                {"field": "year_range", "variable name": "year_range", "type": "Custom", "comparator": cf.compare_range_from_tuple_tempfix, "has missing": True},
                {"field": "year_range", "variable name": "year_range", "type": "Custom", "comparator": cf.compare_startrange_from_tuple, "has missing": True},
                {"field": "year_range", "variable name": "year_range", "type": "Custom", "comparator": cf.compare_endrange_from_tuple, "has missing": True}
            ] 
     
        if args.institution == "True":
            if args.linking_type == "graduates": # should we also ignore uni for graduates?
                fields.append({"field": "institution", "variable name": "institution", "type": "Custom", "comparator": cf.tuple_distance, "has missing": True})
            else:
                if args.linking_type == "grants": 
                    # TODO: the naming of the insitution fields below is a typo and creates the wrong interactions later on
                        # Leaving this here for now for consistency with current advisor linking approach 
                        # 1. rename the variables:
                            # main_inst_year_similarity (and `all_us_*``) should use cf.set_of_tuples_distance_overall; 
                            # main_inst_year (and `all_us_*``) should use cf.set_of_tuples_distance_number
                        # see below for advisors; I think we can just use the same for both advisors and grants
                    institution_fields = [ 
                        {"field": "main_us_institutions_year", "variable name": "main_inst_year", "type": "Custom", "comparator": cf.set_of_tuples_distance_overall, "has missing": True},
                        {"field": "main_us_institutions_year", "variable name": "main_inst_similarity", "type": "Custom", "comparator": cf.set_of_tuples_distance_string, "has missing": True},
                        {"field": "main_us_institutions_year", "variable name": "main_inst_year_similarity", "type": "Custom", "comparator": cf.set_of_tuples_distance_number, "has missing": True},
                        {'type': 'Interaction', 'interaction variables': ['main_inst_year_similarity', 'firstname']}, 
                        {'type': 'Interaction', 'interaction variables': ['main_inst_year_similarity', 'lastname']}, 
                        {'type': 'Interaction', 'interaction variables': ['main_inst_year_similarity', 'firstname']}, 
                        {'type': 'Interaction', 'interaction variables': ['main_inst_year_similarity', 'lastname']},
                        {"field": "all_us_institutions_year", "variable name": "all_inst_year", "type": "Custom", "comparator": cf.set_of_tuples_distance_overall, "has missing": True},
                        {"field": "all_us_institutions_year", "variable name": "all_inst_similarity", "type": "Custom", "comparator": cf.set_of_tuples_distance_string, "has missing": True},
                        {"field": "all_us_institutions_year", "variable name": "all_inst_year_similarity", "type": "Custom", "comparator": cf.set_of_tuples_distance_number, "has missing": True},
                        {"type": "Interaction", "interaction variables": ["main_inst_year_similarity", "year_range"]} # year_range is the career; main_inst is only US. the interaction may account for people that have part of their career outside of US institutions
                        ]
                elif args.linking_type == "advisors":
                    institution_fields = [ 
                        {"field": "main_us_institutions_year", "variable name": "main_year_similarity", "type": "Custom", "comparator": cf.set_of_tuples_distance_number, "has missing": True},
                        {"field": "main_us_institutions_year", "variable name": "main_inst_similarity", "type": "Custom", "comparator": cf.set_of_tuples_distance_string, "has missing": True},
                        {"field": "main_us_institutions_year", "variable name": "main_inst_year_similarity", "type": "Custom", "comparator": cf.set_of_tuples_distance_overall, "has missing": True},
                        # interaction 1
                        {'type': 'Interaction', 'interaction variables': ['main_year_similarity', 'firstname']}, 
                        {'type': 'Interaction', 'interaction variables': ['main_year_similarity', 'lastname']}, 
                        # interaction 2
                        {'type': 'Interaction', 'interaction variables': ['main_inst_similarity', 'firstname']}, 
                        {'type': 'Interaction', 'interaction variables': ['main_inst_similarity', 'lastname']},
                        {"type": "Interaction", "interaction variables": ["main_inst_year_similarity", "year_range"]},
                        {"field": "all_us_institutions_year", "variable name": "all_year_similarity", "type": "Custom", "comparator": cf.set_of_tuples_distance_number, "has missing": True},
                        {"field": "all_us_institutions_year", "variable name": "all_inst_similarity", "type": "Custom", "comparator": cf.set_of_tuples_distance_string, "has missing": True},
                        {"field": "all_us_institutions_year", "variable name": "all_inst_year_similarity", "type": "Custom", "comparator": cf.set_of_tuples_distance_overall, "has missing": True}
                        ]
                        
                fields = fields + institution_fields 
        if args.fieldofstudy_cat == "True": 
            fields.append({"field": "fieldofstudy", "variable name": "fieldofstudy", "type": "Categorical", "categories": areas, "has missing": False})
        if args.fieldofstudy_str == "True": 
            fields.append({"field": "fieldofstudy", "variable name": "fieldofstudy", "type": "String", "has missing": False}) 
        if args.keywords == "True": 
            fields.append({"field": "keywords", "variable name": "keywords", "type": "Custom", "comparator": cf.keyword_comparator ,"has missing": True})
        
        linker = dedupe.RecordLink(fields, num_cores = n_cores)

        # Define data_1 and data_2 depending on linking input. This is important when making many-to-one links
        if args.linking_type == "graduates":
            data_1_use = magdata 
            data_2_use = otherdata
        elif args.linking_type == "advisors":
            data_1_use = otherdata
            data_2_use = magdata
        elif args.linking_type == "grants":
            data_1_use = otherdata
            data_2_use = magdata 
            
        del otherdata 
        del magdata

        if os.path.exists(training_file):
            print(f"Reading labelled examples from {training_file}", flush=True)
            with open(training_file) as tf:
                linker.prepare_training(
                    data_1 = data_1_use, 
                    data_2 = data_2_use, 
                    training_file = tf,
                    blocked_proportion = share_blockedpairs_training,
                    sample_size = args.samplesize
                )
            n_match = len(linker.training_pairs["match"])
            n_distinct = len(linker.training_pairs["distinct"])
        else:
            linker.prepare_training(
                data_1 = data_1_use, 
                data_2 = data_2_use,
                blocked_proportion = share_blockedpairs_training,
                sample_size = args.samplesize
            )

        print_elapsed_time(start_time)
        print("Starting active labeling...", flush=True)

        dedupe.console_label(linker)
        linker.train(recall = args.recall)

        with open(training_file, "w") as tf: 
            linker.write_training(tf)
        
        with open(settings_file, "wb") as sf: 
            linker.write_settings(sf)
        
        n_match = len(linker.training_pairs["match"])
        n_distinct = len(linker.training_pairs["distinct"])
    
        linker.cleanup_training()

    for c in [read_con, write_con, read_dict_con]:
        c.close()

    time_to_run = (time.time() - start_time)/60

    print(f"Done in {time_to_run} minutes.")