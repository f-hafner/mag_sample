#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script topic_svd_similarity.py
Calculate topic similarity for linked graduates using SVD embeddings
    - between their topics pre and post phd
    - between potential employers
        - to faculty average
        - to most similar faculty member
This script uses the SVD model to transform the topic vectors before computing similarities.
"""

import sqlite3 as sqlite
import time
from pathlib import Path
import pandas as pd

from helpers.variables import db_file, insert_questionmark_doctypes, keep_doctypes
from helpers.functions import enumerated_arguments
import argparse
import logging
import os
import sys
import multiprocessing as mp
import itertools
from concurrent.futures import ProcessPoolExecutor
import warnings
from pickle import load

import main.link.topic_similarity_functions as tsf

logging.basicConfig(level=logging.INFO)

def parse_args():
    parser = argparse.ArgumentParser(description='Inputs for topic_svd_similarity')
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
                        default="similarities_svd_temp/")
    parser.add_argument("--limit",
                        type=int,
                        default=None,
                        help="Limit number of field-degree year combinations to process. For quick testing.")
    parser.add_argument("--max-level",
                        type=int,
                        default=2,
                        help="Use fields of study up to this level (included) for computing the concept vectors")
    parser.add_argument("--model-path",
                        type=str,
                        default="/mnt/ssd/AcademicGraph/svd_model.pkl",
                        help="Path to the saved SVD model")
    parser.add_argument('--parallel', action=argparse.BooleanOptionalAction, dest="parallel")
    args = parser.parse_args()
    return args

def load_svd_model(model_path):
    logging.info(f"Loading SVD model from {model_path}")
    with open(model_path, "rb") as f:
        return load(f)

def get_svd_similarities(data):
    chunk_id, dbfile, write_dir, degree_year, field, keep_top_n_authors, max_level, window_size, model_path, max_level = data
    con = sqlite.connect(database = "file:" + dbfile + "?mode=ro", isolation_level=None, uri=True)

    logging.debug(f"{chunk_id=}, {degree_year=}, {field=}")
    sql_queries = tsf.QueryBuilder(
        degree_year_to_query=degree_year,
        window_size=window_size,
        field_to_query=field,
        qmarks_doctypes=insert_questionmark_doctypes,
        keep_doctypes=keep_doctypes,
        max_level=max_level
    )

    svd_model = load_svd_model(model_path)
    # load fields of study
    fields_of_study = pd.read_sql("""
                               SELECT * FROM (
                                  SELECT FieldOfStudyId FROM FieldsOfStudy
                               WHERE Level <= ?
                                  ) 
                                ORDER BY FieldOfStudyId""",
                               con=con,
                               params=(max_level,))
    field_to_index = {id: index for index, id in enumerate(fields_of_study['FieldOfStudyId'].unique())}

    with con as c:
        d_affiliations = pd.read_sql(con=c, sql=sql_queries.query_affiliations())

    logging.info("getting student data:")

    student_topics, d_graduates = tsf.get_student_data(con=con, queries=sql_queries)

    logging.info("calculate similarity pre post")


    d_similarity_prepost = tsf.compute_svd_similarity(
        df_A=student_topics.loc[student_topics["period"] == "pre_phd", ["AuthorId", "FieldOfStudyId", "Score"]],
        df_B=student_topics.loc[student_topics["period"] == "post_phd", ["AuthorId", "FieldOfStudyId", "Score"]],
        unit_A=["AuthorId"],
        unit_B=["AuthorId"],
        groupvars=["AuthorId"],
        field_to_index=field_to_index,
        svd_model=svd_model,
        fill_A_units=True
    )

    logging.info("similarity to faculty")
    d_similarity_to_faculty = tsf.similarity_to_faculty_svd(
        d_affiliations=d_affiliations,
        d_graduates=d_graduates,
        student_topics=student_topics,
        queries=sql_queries,
        con=con,
        field_to_index=field_to_index,
        svd_model=svd_model
    )

    logging.info("similarity to closest collaborator")
    d_most_similar_collaborator, highest_similarity_by_institution = tsf.similarity_to_closest_collaborator_svd(
        con=con,
        queries=sql_queries,
        student_topics=student_topics,
        d_affiliations=d_affiliations,
        d_graduates=d_graduates,
        top_n_authors=keep_top_n_authors
    )

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


    logging.info("making dict to be written")

    write_dict = {
        "own_svd": d_similarity_prepost,
        "inst_svd": d_similarity_to_institutions,
        "closest_collaborator_ids_svd": d_most_similar_collaborator.drop(columns=['Field0'])
    }

    logging.info("writing dict")
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

    if os.path.isdir(write_url):
        sys.exit("You specified an existing directory.")

    start_time = time.time()
    write_url.mkdir(parents=True)

    con = sqlite.connect(database = "file:" + db_file + "?mode=ro", isolation_level=None, uri=True)

    q_fields = "SELECT FieldOfStudyId FROM FieldsOfStudy WHERE Level = 0"
    q_years = """
        SELECT DISTINCT degree_year
        FROM pq_authors
        INNER JOIN (SELECT goid FROM current_links) USING(goid)
    """

    fields = [f[0] for f in con.execute(q_fields).fetchall()]
    years = [y[0] for y in con.execute(q_years).fetchall()]
    con.close()

    inputs = itertools.product(
        [db_file], [f"{str(write_url)}/"], years, fields, [args.top_n_authors],
        [args.max_level], [args.window_size], [args.model_path], [args.max_level]
    )

    enumerated_inputs = enumerated_arguments(inputs, limit=args.limit)
    ctx = mp.get_context("forkserver")
    logging.info("Running queries")
    if not args.parallel:
        inputs = (0, db_file, write_url, 2005, 162324750, args.top_n_authors, args.max_level, args.window_size, args.model_path, args.max_level)
        get_svd_similarities(inputs)
    else:
        with ProcessPoolExecutor(max_workers=args.n_cores, mp_context=ctx) as executor:
            result = executor.map(get_svd_similarities, enumerated_inputs, chunksize=1)

    print("--queries finished.")

    n_expected = len(years) * len(fields)
    files_created = os.listdir(write_url)
    if n_expected * 3 != len(files_created):
        warnings.warn("The number of files created does not match the expected number. Check the output carefully.")

    end_time = time.time()
    print(f"Done in {(end_time - start_time)/60} minutes.")

if __name__ == "__main__":
    main()
