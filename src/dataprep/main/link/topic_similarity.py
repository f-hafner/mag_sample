#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script topic_similarity.py
Calculate topic similarity for linked graduates
    - between their topics pre and post phd
    - between potential employers
        - to faculty average 
        - to most similar faculty member
The mag field0 of the first reported keyword that is successfully
matched to mag field0 is used as the relevant field of study for each graduate: this means 
that for economics graduates, only other economists/economics "departments"
are considered for computing the topic similarities.
"""

import sqlite3 as sqlite
import time 
from pathlib import Path
import pandas as pd
from helpers.variables import db_file, insert_questionmark_doctypes, keep_doctypes
from helpers.functions import enumerated_arguments
import pdb 
import argparse
import logging 
import os 
import sys 
import multiprocessing as mp 
import itertools 
from concurrent.futures import ProcessPoolExecutor
import warnings

import main.link.topic_similarity_functions as tsf


logging.basicConfig(level=logging.INFO)

# ## Arguments


def parse_args():
    parser = argparse.ArgumentParser(description = 'Inputs for topic_similarity')
    parser.add_argument("--ncores", 
                        dest="n_cores", 
                        default=int(mp.cpu_count() / 2),
                        type=int, 
                        help="Number of cores to use. Defaults to half of available CPUs.")
    parser.add_argument("--top_n_authors", 
                        type=int,
                        default=200,
                        help="Keep as many authors for each institution to search for most similar faculty member.") 
    parser.add_argument("--window_size",
                        type=int,
                        default=5,
                        help="Calculate topics based on as many papers before or after graduation year.")
    parser.add_argument("--write_dir", 
                        dest="write_dir", 
                        default="similarities_temp/")
    parser.add_argument("--limit",
                        type=int,
                        default=None,
                        help="Limit number of field-degree year combinations to process. For quick testing.")
    parser.add_argument("--max_level",
                        type=int,
                        default=5,
                        help="Use fields of study up to this level (included) for computing the conept vectors")
    parser.add_argument('--parallel', action=argparse.BooleanOptionalAction, dest="parallel")
    args = parser.parse_args()
    return args



def get_similarities(data):
    """Calculate similarities between graduates from `degree_year` in `field`
    to their potential employers (on average/to closest potential collaborator), 
    and to themselves (pre/post graduation).
    The results are saved as csv for further processing.

    Parameters:
    ----------
    chunk_id: int
        Identifier of the chunk
    dbfile: str
        location of database to query
    write_dir: str
        path to store processed results
    degree_year: int
        degree year to process
    field: str
        field of study level 0 to process
    keep_top_n_authors: int
        keep as many authors, ordered by number of publications,
        when calculating the most simlar author at a given institution-field.
    max_level: int. Use fields of study up to this level for creating
        the concept vector.
    window_size: int
    """
    chunk_id, dbfile, write_dir, degree_year, field, keep_top_n_authors, max_level, window_size = data 
    con = sqlite.connect(database = "file:" + dbfile + "?mode=ro", 
                         isolation_level=None, 
                         uri=True) # read-only connection 

    logging.debug(f"{chunk_id=}, {degree_year=}, {field=}")
    sql_queries = tsf.QueryBuilder(
        degree_year_to_query=degree_year,
        window_size=window_size,
        field_to_query=field,
        qmarks_doctypes=insert_questionmark_doctypes,
        keep_doctypes=keep_doctypes,
        max_level=max_level
    )

    with con as c:
        d_affiliations = pd.read_sql(
            con=c, 
            sql=sql_queries.query_affiliations()
        )

    ## stuff for main function 
    logging.info("getting student data")
    student_topics, d_graduates = tsf.get_student_data(
        con=con, 
        queries=sql_queries
        )

    d_similarity_prepost = tsf.compute_similarity(
        df_A=student_topics.loc[
            student_topics["period"] == "pre_phd", 
            ["AuthorId", "FieldOfStudyId", "Score"]
            ],
        df_B=student_topics.loc[
            student_topics["period"] == "post_phd",
            ["AuthorId", "FieldOfStudyId", "Score"]
        ],
        unit_A=["AuthorId"],
        unit_B=["AuthorId"],
        groupvars=["AuthorId"],
        fill_A_units=True
    )

    # ### Topic similarity between graduate and average faculty 
    logging.info("similarity to faculty")

    d_similarity_to_faculty = tsf.similarity_to_faculty(
        d_affiliations=d_affiliations,
        d_graduates=d_graduates,
        student_topics=student_topics,
        queries=sql_queries,
        con=con
    )


    # ### Topic similarity between graduate and most similar faculty member 
    logging.info("similarity to closest collaborator")
    d_most_similar_collaborator, highest_similarity_by_institution = tsf.similarity_to_closest_collaborator(
        con=con,
        queries=sql_queries,
        student_topics=student_topics,
        d_affiliations=d_affiliations,
        d_graduates=d_graduates,
        top_n_authors=keep_top_n_authors
    )

    # ## Make one df for similarity to institutions
    logging.info("making d_similarity_institutions")

    idx_vars = ["AuthorId", "AffiliationId", "period", "Field0"]
    d_similarity_to_institutions = (
        d_similarity_to_faculty
            .set_index(idx_vars)
            .rename(columns={"sim": "similarity_faculty_overall"})
            .join(highest_similarity_by_institution
                    .set_index(idx_vars)
                    .rename(columns={"sim": "similarity_closest_collaborator"}))
            .reset_index()
            .drop(columns=["Field0"])
    )

    write_dict = {
        "own": d_similarity_prepost,
        "inst": d_similarity_to_institutions,
        "closest_collaborator_ids": d_most_similar_collaborator.drop(columns=['Field0'])
    }

    for name, df in write_dict.items():
        df["max_level"] = max_level
        df.to_csv(f"{write_dir}/{name}-part-{chunk_id}.csv", index=False)

    logging.debug("Done with one chunk.")

    con.close()



def main():
    args = parse_args()
    write_url = Path(args.write_dir, f"maxlevel-{args.max_level}") 
    logging.debug(f"{write_url=}")

    if args.n_cores > mp.cpu_count():
        print("Specified too many cpu cores.")
        print(f"Using max available, which is {mp.cpu_count()}.")
        args.n_cores = mp.cpu_count()

    if os.path.isdir(args.write_dir):
        sys.exit("You specified an existing directory: " + args.write_dir)

    # ## Setup
    start_time = time.time()    
   
    write_url.mkdir(parents=True)

    # ## Prepare inputs for mp 
    con = sqlite.connect(database = "file:" + db_file + "?mode=ro", 
                        isolation_level=None, 
                        uri=True) # read-only connection 

    # we need all years, all fields level 0 
    q_fields = """
        SELECT FieldOfStudyId
        FROM FieldsOfStudy
        WHERE Level = 0
    """

    q_years = """
        SELECT DISTINCT degree_year 
        FROM pq_authors
        INNER JOIN (
            SELECT goid
            FROM current_links
        ) USING(goid)
    """

    fields = con.execute(q_fields).fetchall()
    fields = [f[0] for f in fields]
    years = con.execute(q_years).fetchall()
    years = [y[0] for y in years]
    con.close()
    

    inputs = itertools.product(
        [db_file], [f"{str(write_url)}/"], years, fields, [args.top_n_authors], [args.max_level], [args.window_size]
        )

    enumerated_inputs = enumerated_arguments(inputs, limit=args.limit)
    #logging.debug(f"{list(enumerated_inputs)=}")
    ctx = mp.get_context("forkserver")
    logging.info("Running queries")
    if not args.parallel:
        inputs = (0, db_file, write_url, 2005, 162324750, args.top_n_authors, args.max_level, args.window_size)
        get_similarities(inputs)
    else:
        with ProcessPoolExecutor(max_workers=args.n_cores, mp_context=ctx) as executor:
            result = executor.map(get_similarities, enumerated_inputs, chunksize=1)

    print("--queries finished.")

    # check number of created files is as expected
    n_expected = len(years) * len(fields)
    files_created = os.listdir(write_url)
    if n_expected * 3 != len(files_created):
        warnings.warn("The number of files created does not match the expected number. Check the output carefully.")

    # ## Finish
    end_time = time.time()
    print(f"Done in {(end_time - start_time)/60} minutes.")



if __name__ == "__main__":
    main()

