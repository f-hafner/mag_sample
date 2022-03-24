"""
Step 1 to Link MAG authors to ProQuest graduates:
         Active labelling
"""

from setup_linking import *


print("finished setup ... ", flush=True)

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


    # ## prepare the keywords from proquest
    print("Preparing temp tables for info of proquest authors... \n")
    # ### keywords
    write_con.execute("DROP TABLE IF EXISTS pq_keywords")
    write_con.execute("""
    CREATE TABLE pq_keywords AS 
    SELECT goid, GROUP_CONCAT(fieldname, ";") as keywords
    FROM pq_fields 
    GROUP BY goid
    """)
    write_con.execute("CREATE UNIQUE INDEX idx_kw_goid ON pq_keywords(goid ASC)")

    ### advisors -> CURRENTLY UNUNSED
    write_con.execute("DROP TABLE IF EXISTS pq_all_advisors")
    write_con.execute("""
    CREATE TABLE pq_all_advisors AS
    SELECT goid, GROUP_CONCAT(fullname, ";") AS advisors
    FROM (
        SELECT goid, firstname || " " || lastname as fullname 
        FROM pq_advisors
    )
    GROUP BY goid
    """)
    write_con.execute("CREATE UNIQUE INDEX idx_aa_goid ON pq_all_advisors (goid ASC)")

    # ## Load data 
   

    if args.testing:
        line_limit = 500
        query_proquest = f"{query_proquest} LIMIT {line_limit}"
        query_mag = f"{query_mag} LIMIT {line_limit}"

    for q in [query_proquest, query_mag]:
        print(f"{q} \n")

    # https://stackoverflow.com/questions/3300464/how-can-i-get-dict-from-sqlite-query
    # https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.row_factory

    with read_dict_con as con:
        cur = con.cursor()
        cur.execute(query_mag, tuple(id_field))
        magdata = {i: row for i, row in custom_enumerate(cur.fetchall(), "AuthorId")}
        cur = con.cursor()
        cur.execute(query_proquest, tuple(id_field))
        proquestdata = {i: row for i, row in custom_enumerate(cur.fetchall(), pq_entity_id)} 
    
    # transform the strings to hashable sequences
    for data in [magdata, proquestdata]:
        for key in data.keys():
            if data[key]["keywords"] is not None:
                data[key]["keywords"] = frozenset(data[key]["keywords"].split(";"))

            features = ["institution", "coauthors"]
            ft_in_data = list(data[list(data.keys())[0]].keys()) # extract all features of the first record in the dict data
            features = [f for f in features if f in ft_in_data]
            for feature in features:
                if data[key][feature] is not None:
                    data[key][feature] = tuple(data[key][feature].split(";"))

    # NOTE
        # need `frozenset` for the set feature; while the documentation says tuples also work, there is a bug 
        # in dedupe for reading tuples from training data
        # (frozensets are encoded in https://github.com/dedupeio/dedupe/blob/e010ba1790b4b9744a74b32a5f762f8eac41f74f/dedupe/serializer.py#L25,
        # but not tuples). Kept tuples for institution and coauthors as it seems more readable for labelling

    n_match = None # this will create null in database when settings were read from settings_file
    n_distinct = None

    if os.path.exists(settings_file):
        print('reading from ', settings_file)
        with open(settings_file, 'rb') as sf:
            linker = dedupe.StaticRecordLink(sf, num_cores = n_cores)
    else:
        # define fields for categorical 
        query_fields_mag = f"SELECT DISTINCT(fieldofstudy) FROM ( {query_mag} ) WHERE fieldofstudy IS NOT NULL"
        query_fields_proquest = f"SELECT DISTINCT(fieldofstudy) FROM ( {query_proquest} ) WHERE fieldofstudy IS NOT NULL"
        mag_areas = [i[0] for i in read_con.execute(query_fields_mag, tuple(id_field)).fetchall()] 
        proquest_areas = [i[0] for i in read_con.execute(query_fields_proquest, tuple(id_field)).fetchall()] 
        areas = mag_areas + proquest_areas

        if args.linking_type == "graduates":
            fields = [
                {"field": "firstname", "variable name": "firstname", "type": "String", "has missing": False},
                {"field": "firstname", "variable name": "same_firstname", "type": "Custom", "comparator": name_comparator},
                {"field": "lastname", "variable name": "lastname", "type": "String", "has missing": False},
                {"field": "lastname", "variable name": "same_lastname", "type": "Custom", "comparator": name_comparator},
                {"field": "middlename", "variable name": "middlename", "type": "String", "has missing": True},
                # {"field": "middle_lastname", "variable name": "same_name", "type": "Custom", "comparator": name_comparator},
                {"field": "year", "variable name": "year", "type": "Price"},
                # {"field": "year", "variable name": "no_advisor_info", "type": "Custom", "comparator": year_dummy_noadvisor},
                # {"field": "year", "variable name": "yeardiff_sqrd", "type": "Custom", "comparator": squared_diff},
                {"type": "Interaction", "interaction variables": ["year", "same_firstname"]},
                {"type": "Interaction", "interaction variables": ["year", "same_lastname"]},
                # {"field": "coauthors", "variable name": "coauthors", "type": "Custom", "comparator": max_set_similarity, "has missing": True}, 
                # {"type": "Interaction", "interaction variables": ["no_advisor_info", "coauthors"], "has missing": True},
            ]
        elif args.linking_type == "advisors":
            fields = [
                {"field": "firstname", "variable name": "firstname", "type": "String", "has missing": False},
                {"field": "firstname", "variable name": "same_firstname", "type": "Custom", "comparator": name_comparator},
                {"field": "lastname", "variable name": "lastname", "type": "String", "has missing": False},
                {"field": "lastname", "variable name": "same_lastname", "type": "Custom", "comparator": name_comparator},
                {"field": "middlename", "variable name": "middlename", "type": "String", "has missing": True},
            ]
     
        if args.institution == "True":
            if args.linking_type == "graduates": # should we also ignore uni for graduates?
                fields.append({"field": "institution", "variable name": "institution", "type": "Custom", "comparator": max_set_similarity, "has missing": True})
            elif args.linking_type == "advisors":
                fields.append({"field": "institution", "variable name": "institution", "type": "Custom", "comparator": max_set_similarity_ignoreuni, "has missing": True})
                fields.append({"type": "Interaction", "interaction variables": ["institution", "same_firstname"] })
                fields.append({"type": "Interaction", "interaction variables": ["institution", "same_lastname"] })
        if args.fieldofstudy_cat == "True": 
            fields.append({"field": "fieldofstudy", "variable name": "fieldofstudy", "type": "Categorical", "categories": areas, "has missing": False})
        if args.fieldofstudy_str == "True": 
            fields.append({"field": "fieldofstudy", "variable name": "fieldofstudy", "type": "String", "has missing": False}) 
        if args.keywords == "True": 
            fields.append({"field": "keywords", "variable name": "keywords", "type": "Set", "has missing": True})
        
        linker = dedupe.RecordLink(fields, num_cores = n_cores)

        # Define data_1 and data_2 depending on linking input. This is important when making many-to-one links
        if args.linking_type == "graduates":
            data_1_use = magdata 
            data_2_use = proquestdata
        elif args.linking_type == "advisors":
            data_1_use = proquestdata
            data_2_use = magdata
            
        del proquestdata 
        del magdata

        if os.path.exists(training_file):
            print(f"Reading labelled examples from {training_file}")
            with open(training_file) as tf:
                linker.prepare_training(
                    data_1 = data_1_use, 
                    data_2 = data_2_use, 
                    training_file = tf,
                    blocked_proportion = share_blockedpairs_training,
                    sample_size = 100_000
                )
            n_match = len(linker.training_pairs["match"])
            n_distinct = len(linker.training_pairs["distinct"])
        else:
            linker.prepare_training(
                data_1 = data_1_use, 
                data_2 = data_2_use,
                blocked_proportion = share_blockedpairs_training,
                sample_size = 100_000
            )

        print("Starting active labeling...")

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