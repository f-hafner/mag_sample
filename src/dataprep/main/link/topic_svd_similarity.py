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
import numpy as np
from scipy.sparse import csr_matrix
from sklearn.metrics.pairwise import cosine_similarity
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
import main.link.similarity_helpers as sim_helpers
import main.link.fit_svd_model as fit_svd

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
    parser.add_argument("--max_level",
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

"""
Transform topic vectors using the SVD model.
Args:
    topics_df (pd.DataFrame): DataFrame containing topic vectors
    field_to_index (dict): Mapping of field IDs to matrix indices
    svd_model (object): Trained SVD model
Returns:
    np.array: Transformed topic vectors
"""
def transform_topics(topics_df, field_to_index, svd_model):
    sparse_matrix, _ = fit_svd.make_sparse(topics_df, field_to_index, "AuthorId", "FieldOfStudyId", "Score")
    return svd_model.transform(sparse_matrix)

def similarity_to_faculty_svd(
        d_affiliations, 
        d_graduates,
        student_topics,
        queries,
        con,
        field_to_index,
        svd_model
    ):
    """Calculate similarity between student SVD embeddings and overall faculty SVD embeddings.

    Parameters:
    -----------
    d_affiliations: dataframe with hiring AffiliationIds 
    d_graduates: dataframe with goid, AuthorId, degree year and Field0
    student_topics: dataframe with scores by AuthorId, FieldOfStudyId, period and Field0
    queries: QueryBuilder instance
    con: sqlite connection
    field_to_index: Mapping of field IDs to matrix indices
    svd_model: Trained SVD model
    """

    # Get affiliation topics 
    with con as c:
        df_fields = pd.read_sql(con=c, sql=queries.query_affiliation_topics())
    
    df_fields = sim_helpers.split_year_pre_post(df=df_fields, ref_year=queries.degree_year_to_query)

    affiliation_topics = (df_fields
        .groupby(["AffiliationId", "Field0", "FieldOfStudyId", "period"])
        .agg({"Score": np.sum})
        .reset_index()
        )


    # Calculate similarity 
    d_sim = compute_svd_similarity(
        df_A=student_topics, 
        df_B=affiliation_topics,
        unit_A=["AuthorId"],
        unit_B=["AffiliationId"], 
        groupvars=["period", "Field0"],
        field_to_index=field_to_index,
        svd_model=svd_model)

    # "reference" table 
    d_graduates_affiliations = tsf.make_student_affiliation_table(
        d_affiliations=d_affiliations,
        d_graduates=d_graduates
    )
    d_sim = tsf.complete_to_reference(
        df_in=d_sim, 
        df_ref=d_graduates_affiliations,
        idx_cols=["AuthorId", "AffiliationId"], 
        add_cols_to_complete=["period"]
    )

    return d_sim


def compute_svd_similarity(df_A, df_B, unit_A, unit_B, groupvars, field_to_index, svd_model, fill_A_units=False):
    """
    Compute similarity between two sets of topic vectors using SVD embeddings.
    
    Args:
        df_A (pd.DataFrame): First set of topic vectors
        df_B (pd.DataFrame): Second set of topic vectors
        unit_A (list): Columns identifying units in df_A
        unit_B (list): Columns identifying units in df_B
        groupvars (list): Additional grouping variables
        field_to_index (dict): Mapping of field IDs to matrix indices
        svd_model (object): Trained SVD model
        fill_A_units (bool): Whether to fill missing units in df_A with zero similarity
    Returns:
        pd.DataFrame: Computed similarities
    """
    A_transformed = transform_topics(df_A, field_to_index, svd_model)
    B_transformed = transform_topics(df_B, field_to_index, svd_model)

    # Aggregate embeddings at the group level
    A_aggregated = pd.DataFrame(A_transformed, index=df_A[unit_A + groupvars].drop_duplicates()).groupby(unit_A + groupvars).sum()
    B_aggregated = pd.DataFrame(B_transformed, index=df_B[unit_B + groupvars].drop_duplicates()).groupby(unit_B + groupvars).sum()

    sim_matrix = cosine_similarity(A_aggregated, B_aggregated)

    d_sim = pd.DataFrame(sim_matrix, columns=B_aggregated.index, index=A_aggregated.index)
    d_sim = d_sim.stack().reset_index()
    d_sim.columns = unit_A + unit_B + groupvars + ['sim']

    if fill_A_units:
        required_ids = pd.DataFrame(df_A[unit_A[0]].unique(), columns=unit_A)
        d_sim = required_ids.merge(d_sim, on=unit_A, how='left')
        d_sim['sim'] = d_sim['sim'].fillna(0)

    return d_sim

def get_svd_similarities(data):
    chunk_id, dbfile, write_dir, degree_year, field, keep_top_n_authors, max_level, window_size, model_path = data
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
    field_to_index = fit_svd.get_field_to_index(con)

    with con as c:
        d_affiliations = pd.read_sql(con=c, sql=sql_queries.query_affiliations())

    logging.info("getting student data")
    student_topics, d_graduates = tsf.get_student_data(con=con, queries=sql_queries)

    d_similarity_prepost = compute_svd_similarity(
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
    d_similarity_to_faculty = similarity_to_faculty_svd(
        d_affiliations=d_affiliations,
        d_graduates=d_graduates,
        student_topics=student_topics,
        queries=sql_queries,
        con=con,
        field_to_index=field_to_index,
        svd_model=svd_model
    )

    logging.info("similarity to closest collaborator")
    d_most_similar_collaborator, highest_similarity_by_institution = tsf.similarity_to_closest_collaborator(
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
        [args.max_level], [args.window_size], [args.model_path]
    )

    enumerated_inputs = enumerated_arguments(inputs, limit=args.limit)
    ctx = mp.get_context("forkserver")
    logging.info("Running queries")
    if not args.parallel:
        inputs = (0, db_file, write_url, 2005, 162324750, args.top_n_authors, args.max_level, args.window_size, args.model_path)
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
