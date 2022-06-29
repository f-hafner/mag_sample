#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Script read_collab.py
Read the files, outputted in prep_collab.py, into the database with the following steps:
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

parser = argparse.ArgumentParser(description = 'Inputs for author_collab')
parser.add_argument("--read_dir", dest="read_dir", default = "collab_temp/")

args = parser.parse_args()
read_dir = args.read_dir

filename_full = "collab_full.csv"


start_time = time.time()

con = sqlite.connect(database = db_file, isolation_level= None)


# todo: fix the directory for tempfile and for partial files
print("Combining files into one", flush = True)
subprocess.run(f"tail -n +2 -q {read_dir}/part-*.csv >> {filename_full}", shell = True)

print("Dropping existing table and creating new empty one", flush = True)
con.execute("DROP TABLE If EXISTS author_collab")
con.execute("CREATE TABLE author_collab (AuthorId INTEGER, CoAuthorId INTEGER, Year INTEGER)")


print("Reading into sqlite", flush = True)
subprocess.run(
    ["sqlite3", db_file,
    ".mode csv",
    f".import {filename_full} author_collab"]
)

os.remove(filename_full)
shutil.rmtree(read_dir)

print("Creating indexes", flush = True)
con.execute("CREATE UNIQUE INDEX idx_acllb_AuthorCoAuthorYear ON author_collab (AuthorId ASC, CoAuthorId ASC, Year)")
con.execute("CREATE INDEX idx_acllb_CoAuthorIdYear ON author_collab (CoAuthorId ASC, Year)")
con.execute("CREATE INDEX idx_acllb_AuthorIdYear ON author_collab (AuthorId ASC, Year)")


# ## Run ANALYZE, finish
analyze_db(con)

con.close()

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")


# somehow check the db? 
# this is for checking the results later on 
# collab = pd.read_sql("select distinct authorid from author_collab", con = con)

# authors.loc[~authors.AuthorId.isin(collab.AuthorId)].head() # not all are in there: some do not co-author with anyone 
# authors.loc[authors.AuthorId.isin(collab.AuthorId)].head() 
# collab.loc[collab.AuthorId == 3345] # I am not sure what is the best way to test this? probably with a smaller fake db? or just test the modules?




## old 
#-------------
# next steps 
    # 
    # read into db, check that authors in author_sample are also in collab. is this the stuff that is taking long? 
    # test the performance
# write abstract function: process any sql query in parallel over all/a given number of cpus 
# use db connection that only reads; make new one for writing only when needed.
    # https://docs.python.org/3/library/sqlite3.html#sqlite3.connect
# use other file type?
# check the output: query random sample, test whether the output is the same as in the author_collab table 

# this takes too long, likely because method = None, which does a row-by-row insert

# alternative here: https://stackoverflow.com/questions/28004773/loading-multiple-csv-files-with-sqlite


# then 
# create table author_collab (AuthorId INTEGER, CoAuthorId INTEGER, Year INTEGER);
# .mode csv
# .import all_combined.csv author_collab # this takes about 10 minutes for 5mio unique authorIds. thus, about 40minutes for 20mio authors





