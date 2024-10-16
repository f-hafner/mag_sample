#!/usr/bin/python
# -*- coding: utf-8 -*-

# %%
"""
Read in mag field of study of dissertation abstracts (estimated on TDM studio) and save to database.

Create table in database:
- pq_magfos with the estimated fields of study

pq_magfos data schema is:

goid INTEGER, fieldrank INTEGER, FieldOfStudyId INTEGER, score NUMERIC 
 
fieldrank is the counter within a goid for the fields, fieldrank=1 means this is the field that has the highest score
using this variable one can easily get the top N fields, by where fieldrank<=N 

unique index on goid and FieldOfStudyId (multiple fields per goid)
"""

import subprocess
import sqlite3 as sqlite
import argparse
import os
from os import listdir
from os.path import isfile, join
import pandas as pd
import numpy as np 
import re 

from helpers.variables import db_file, datapath, databasepath
from helpers.functions import analyze_db 


path_magfos = datapath+"/magfos/"

# ## Read files in loop and dump to db

def load_magfos(filepath):
    df = pd.read_csv(filepath, 
                        sep="\t",
                        names=["goid", "fieldrank", "FieldOfStudyId", "score"],
                        dtype = {'goid': int, 'fieldrank': int, 'x3': int, 'x4': float})
    df.drop_duplicates(inplace=True)
    return df

files = [f for f in listdir(path_magfos) if isfile(join(path_magfos, f))]


con = sqlite.connect(database = db_file, isolation_level= None)
with con: 
    for (i,f) in enumerate(files):
        df = load_magfos(path_magfos+f)
        #print(df.head())
        if i==0:
            if_exists_opt="replace"
        else:
            if_exists_opt="append"

        df.to_sql("pq_magfos", 
                        con=con, 
                        if_exists=if_exists_opt, 
                        index=False, 
                        schema= """goid INTEGER
                                    , fieldrank INTEGER
                                    , FieldOfStudyId INTEGER
                                    , score NUMERIC
                                """
                    )

    # Make index and clean up
    con.execute("CREATE UNIQUE INDEX idx_pq_magfos ON pq_magfos (goid ASC, FieldOfStudyId ASC)")
    con.execute("CREATE INDEX idx_pq_magfos_fos ON pq_magfos (FieldOfStudyId ASC)")

    analyze_db(con)

con.close()

