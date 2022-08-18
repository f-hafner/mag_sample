"""
Step 2 to Link MAG authors to ProQuest graduates:
        Read labelled examples and predict links.
        Write links to DB
"""

# TODO: dynamic table names for storing links and linking info
# TODO: correctly assign the ids from the theses

from main.link.setup_linking import *

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

    # ### advisors
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
    if args.linking_type == "grants":
        query_other = query_nsf
    else:
        query_other = query_proquest

    if args.testing:
        line_limit = 500
        query_other = f"{query_other} LIMIT {line_limit}"
        query_mag = f"{query_mag} LIMIT {line_limit}"

    for q in [query_other, query_mag]:
        print(f"{q} \n")


    # https://stackoverflow.com/questions/3300464/how-can-i-get-dict-from-sqlite-query
    # https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.row_factory

    # ## Alternatives 
        # this works now, but I think it still loads the data into memory?? how can I prevent this?
        # also, will need a third connection for the dict_factory (otherwise fail below for the areas)
        # compare to their example -- what makes the difference? the context handler? the fetchall()? also read up on server-side cursors in postgre sql and 
            # what is the equivalent for sqlite 
        # how can I know whether something is in memory or not? 
        # see the examples -- it seems that they DO load all the data for the training? double check what Chris understands here. then do it by 10-year intervals

 
    with read_dict_con as con:
        cur = con.cursor()
        cur.execute(query_mag, tuple(id_field))
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
                data[key]["keywords"] = frozenset(data[key]["keywords"].split(";"))

            features = ["institution", "coauthors", "year_range",
                        "main_us_institutions_year", "all_us_institutions_year"]
            ft_in_data = list(data[list(data.keys())[0]].keys()) # extract all features of the first record in the dict data
            features = [f for f in features if f in ft_in_data]
            for feature in features:
                if data[key][feature] is not None:
                    if feature in ["main_us_institutions_year", "all_us_institutions_year"]:
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

    n_match = None # this will create null in database when settings were read from settings_file
    n_distinct = None

    print('reading from: ', settings_file, flush=True)
    with open(settings_file, 'rb') as sf:
        linker = dedupe.StaticRecordLink(sf, num_cores = n_cores)

    print("Link now ... ", flush=True)
    if args.linking_type == "graduates":
        pairs = linker.pairs(data_1 = magdata, data_2 = otherdata)
    elif args.linking_type == "advisors": # this is important: we link many theses in proquest to one record on mag 
        pairs = linker.pairs(data_1 = otherdata, data_2 = magdata)
    elif args.linking_type == "grants":
        pairs = linker.pairs(data_1 = magdata, data_2 = otherdata)

    
    print("made pairs", flush=True)
    scores = linker.score(pairs)
    print("calculated scores", flush=True)
    if args.mergemode=="m:1":
        links = linker.many_to_one(scores, threshold = 0)
        print("made m:1 links", flush=True)
    else:
        links = linker.one_to_one(scores, threshold = 0)
        print("made 1:1 links", flush=True)

    
    del otherdata 
    del magdata

    # ## Write everything into two tables

    print("Writing to database...")

    # ### Prepare tables 
    write_con.execute(f"""
        CREATE TABLE IF NOT EXISTS {tbl_linked_ids}(
            {column_order_links}
            , link_score REAL
            , iteration_id INT)
    """)

 
    write_con.execute(f"""
    CREATE TABLE IF NOT EXISTS {tbl_linking_info}(
        iteration_id INT
        , field TEXT
        , recall REAL
        , startyear INT
        , endyear INT
        , date TEXT
        , testing INT
        , n_match INT
        , n_distinct INT
        , train_name TEXT
        , institution INT
        , fieldofstudy_cat INT
        , fieldofstudy_str INT
        , keywords INT
        , mergemode TEXT
    )
    """)

    last_iteration = read_con.execute(f"SELECT MAX(iteration_id) FROM {tbl_linking_info}").fetchall()[0][0]
    if last_iteration is None:
        iteration_id = 1
    else:
        iteration_id = last_iteration + 1

    # HERE FLAVIO DELETED LINKS FROM OLD RUNS. SEE link_mag_proquest.py if you want to readd it.
    # ### Write links 
    write_con.executemany(
        f"INSERT INTO {tbl_linked_ids} VALUES (?, ?, ?, ?)",
        tupelize_links(links, iteration_id)
    )
    write_con.commit()


    # ### Write iteration info
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    write_con.execute(f"""
    INSERT INTO {tbl_linking_info} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?,?)
    """,
    (iteration_id, field_to_store,
        args.recall, args.startyear, args.endyear, current_time, 
        args.testing, n_match, n_distinct,
        args.train_name, args.institution, args.fieldofstudy_cat, args.fieldofstudy_str, args.keywords, args.mergemode)
    )
    write_con.commit()

    # ## Check fraction matched. Currently only for graduates.
    if args.linking_type == "graduates":
        where_stmt_links = f"WHERE iteration_id = '{iteration_id}'"
        n_graduates = read_con.execute(
                f"""SELECT COUNT(DISTINCT goid) 
                FROM pq_fields_mag 
                INNER JOIN (
                    SELECT goid, university_id
                    FROM pq_authors
                    WHERE degree_year >= {args.startyear}
                        AND degree_year <= {args.endyear}
                ) USING(goid)
                INNER JOIN ( -- only look at graduates from U.S.
                    SELECT university_id
                    FROM pq_unis
                    WHERE location like "%United States%"
                ) USING(university_id)
                WHERE mag_field0 IN ({insert_field_questionmarks}) """,  
                tuple(id_field)
            ).fetchall()[0][0]
        n_links = read_con.execute(f"SELECT COUNT(*) FROM {tbl_linked_ids} {where_stmt_links}").fetchall()[0][0]
        print(f"Found {n_links} links for {n_graduates} graduates with a score of at least 0")
    
    read_con.commit()
    write_con.commit()

    write_con.execute("DROP TABLE IF EXISTS pq_keywords")
    write_con.execute("DROP TABLE IF EXISTS pq_all_advisors")

    # ## Run ANALYZE, finish 
    analyze_db(write_con)

    for c in [read_con, write_con, read_dict_con]:
        c.close()

    time_to_run = (time.time() - start_time)/60

    print(f"Done in {time_to_run} minutes.")