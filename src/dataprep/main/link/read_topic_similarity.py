#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Script read_topic_similarity.py
Read the files, outputted in topic_similarity.py, into the database with the following steps:
    - delete existing table if necessary
    - using subprocess, collect files into one
    - using subprocess and sqlite command line, read into db
    - put indexes on table
"""


import sqlite3 as sqlite
import time 
import argparse
import subprocess
import time 
import os 
import shutil
import logging 

from helpers.functions import analyze_db
from helpers.variables import db_file 

logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser(description = 'Inputs for read_topic_similarity')
parser.add_argument("--read_dir", dest="read_dir", default = "similarities_temp/")

args = parser.parse_args()


start_time = time.time()

con = sqlite.connect(database = db_file, isolation_level= None)


logging.info("Combining files into one")
file_map = {
    "inst": {
        "fn_full": "sim_to_institutions_full.csv",
        "tbl": "graduates_similarity_to_institutions",
        "schema": """(AuthorId INT 
            , AffiliationId INT 
            , period TEXT
            , similarity_faculty_overall REAL
            , similarity_closest_collaborator REAL
            , max_level INT)""",
        "idx": [
            """CREATE UNIQUE INDEX idx_gsi_AuthorAffilPeriod
                ON graduates_similarity_to_institutions (AuthorId, AffiliationId, period)"""
        ]
        },
    "own": {
        "fn_full": "sim_own_full.csv",
        "tbl": "graduates_similarity_to_self",
        "schema": "(AuthorId INTEGER, similarity REAL, max_level INT)",
        "idx": [
            "CREATE UNIQUE INDEX idx_gss_Author ON graduates_similarity_to_self (AuthorId ASC)"
        ]
    },
    "closest_collaborator_ids": {
        "fn_full": "sim_closest_collaborator_full.csv",
        "tbl": "graduates_closest_collaborators",
        "schema": """(AuthorId INT 
            , AffiliationId INT
            , CoAuthorId INT
            , period TEXT
            , similarity REAL
            , max_level INT)""",
        "idx": [
            """CREATE UNIQUE INDEX idx_gcc_AuthorAffilCoAuthorPeriod 
                ON graduates_closest_collaborators(AuthorId ASC, AffiliationId ASC, CoAuthorId ASC, period)"""
            , """CREATE INDEX idx_gcc_AuthorCoAuthor 
                ON graduates_closest_collaborators (AuthorId ASC, CoAuthorId ASC)"""
            ]
    } 
}


for id, params in file_map.items():
    subprocess.run(f"tail -n +2 -q {args.read_dir}/{id}-maxlevel-*-part-*.csv >> {params['fn_full']}", shell=True)
    with con as c:
         c.execute(f"DROP TABLE IF EXISTS {params['tbl']}")
         c.execute(f"CREATE TABLE {params['tbl']} {params['schema']}")
    subprocess.run(
        ["sqlite3", db_file,
        ".mode csv",
        f".import {params['fn_full']} {params['tbl']}"]
    )
    os.remove(params['fn_full'])
    with con as c:
        for idx in params["idx"]:
            c.execute(idx)




# shutil.rmtree(args.read_dir) 


# ## Run ANALYZE, finish
analyze_db(con)

con.close()

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")

