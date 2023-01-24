#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Script read_quantiles_papercites.py
Read the files, outputted in prep_quantiles_papercites.py, into the database with the following steps:
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
parser.add_argument("--read_dir", dest="read_dir", default = "quantiles_temp")

args = parser.parse_args()
read_dir = args.read_dir

filename_full = "quantiles_papercites_full.csv"


start_time = time.time()

con = sqlite.connect(database = db_file, isolation_level= None)


# todo: fix the directory for tempfile and for partial files
print("Combining files into one", flush = True)
subprocess.run(f"tail -n +2 -q {read_dir}/part-*.csv >> {filename_full}", shell = True)

print("Dropping existing table and creating new empty one", flush = True)
con.execute("DROP TABLE If EXISTS quantiles_papercites")
con.execute(
    """ 
    CREATE TABLE quantiles_papercites (
        Year INTEGER
        , Field_lvl1 INTEGER
        , Quantile REAL
        , Value REAL
        , Variable TEXT
    )
    """
)


print("Reading into sqlite", flush = True)
subprocess.run(
    ["sqlite3", db_file,
    ".mode csv",
    f".import {filename_full} quantiles_papercites"]
)

os.remove(filename_full)
shutil.rmtree(read_dir)

print("Creating indexes", flush=True)
with con as c:
    c.execute("CREATE UNIQUE INDEX idx_qpc_FieldYear ON quantiles_papercites (Field_Lvl1 ASC, Year, Quantile)")
    c.execute("CREATE INDEX idx_qp_Year ON quantiles_papercites (Year)")


# ## Run ANALYZE, finish
with con as c:
    analyze_db(c)

con.close()

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")





