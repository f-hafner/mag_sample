#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Script read_quantiles_papercites.py
Read the output files from prep_quantiles_papercites.py into the database with the following steps:
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

from helpers.functions import print_elapsed_time, analyze_db
from helpers.variables import db_file 

parser = argparse.ArgumentParser()
parser.add_argument(dest="read_dirs", nargs="+", help="from which directories to read files")

args = parser.parse_args()

filename_full = "quantiles_papercites_full.csv"
start_time = time.time()

con = sqlite.connect(database = db_file, isolation_level= None)

print("Combining files into one", flush=True)
for dir in args.read_dirs:
    subprocess.run(f"tail -n +2 -q {dir}/part-*.csv >> {filename_full}", shell=True)

print("Dropping existing table and creating new empty one", flush = True)
con.execute("DROP TABLE If EXISTS quantiles_papercites")
con.execute(
    """ 
    CREATE TABLE quantiles_papercites (
        Year INTEGER
        , FieldOfStudyId INTEGER
        , Quantile REAL
        , Value REAL
        , Variable TEXT
    )
    """
)

print("Reading into sqlite", flush=True)
subprocess.run(
    ["sqlite3", db_file,
    ".mode csv",
    f".import {filename_full} quantiles_papercites"]
)

os.remove(filename_full)
for dir in args.read_dirs:
    shutil.rmtree(dir)

print("Creating indexes", flush=True)
with con as c:
    c.execute("CREATE UNIQUE INDEX idx_qpc_FieldYear ON quantiles_papercites (FieldOfStudyId ASC, Year, Quantile)")
    c.execute("CREATE INDEX idx_qp_Year ON quantiles_papercites (Year)")


# ## Run ANALYZE, finish
with con as c:
    analyze_db(c)

con.close()

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")





