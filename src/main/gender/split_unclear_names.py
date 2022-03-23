#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Find the FirstNames without clear gender assignment, and save all parts of the name for these authors in the table AuthorNameSplits.
"""

# ## Notes
# this affects 2.8/16.2 mio authors in author_sample, and 40.6 / 205.8 Mio authors in the overall sample 
# long hispanic names with multiple middle names; maybe last name consisting of multiple names -- what to do?

# what to do with it?
    # genderize the other parts of names 
    # nationalize the other parts of names, use it as additional input to genderize.

# ## TODO
# add names with low counts in genderize?


import sqlite3 as sqlite
import pandas as pd
import time 
import os 
import subprocess

from helpers.variables import db_file, datapath
from helpers.functions import analyze_db

# ## Variables; connect to db
start_time = time.time()
tempfile = datapath + "unclear_names_temp.csv"

con = sqlite.connect(database = db_file, isolation_level= None)

# ## Table with name items per author

query = """
SELECT a.AuthorId, a.NormalizedName, a.FirstName
FROM (
    SELECT AuthorId
            , NormalizedName
            , SUBSTR(TRIM(NormalizedName),1,instr(trim(NormalizedName)||' ',' ')-1) AS FirstName
    FROM Authors
) a
INNER JOIN (
    SELECT FirstName
    FROM FirstNamesGender
    WHERE ProbabilityFemale < 0.8 AND ProbabilityFemale > 0.2
) b ON (a.FirstName = b.FirstName)
"""

print(f"Query: \n--------------------\n {query} \n")

con.execute('DROP TABLE IF EXISTS AuthorNameSplits')

if os.path.exists(tempfile):
  os.remove(tempfile)

header = True
for chunk in pd.read_sql(con = con, sql = query, chunksize=10000000): 
    chunk = chunk.set_index('AuthorId')
    chunk = chunk['NormalizedName'].str.split(expand = True).stack()
    chunk.index.names = ['AuthorId', 'Position']
    chunk = chunk.reset_index().rename(columns = {0: 'Name'})
    chunk.to_csv(path_or_buf = tempfile, mode = "a", header = header, index = False)
    header = False


# ## Read into db
# ## Create table, read in
con.execute('DROP TABLE IF EXISTS AuthorNameSplits')
con.execute("""CREATE TABLE AuthorNameSplits(
                AuthorId TEXT,
                Position INTEGER,
                Name TEXT
                )
            """)

subprocess.run(
    ["sqlite3", db_file,
    ".mode csv", 
    f".import {tempfile} AuthorNameSplits"
    ]
)

con.execute("CREATE INDEX idx_ans_AuthorPosition ON AuthorNameSplits (AuthorId, Position)")


# ## Analyze and finish 
analyze_db(con)

con.close()

os.remove(tempfile)

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")


