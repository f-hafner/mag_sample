#!/usr/bin/python
# -*- coding: utf-8 -*-

# %%
"""
Download SciSciNet table SciSciNet_Link_NSF with links between NSF-grants and papers
Upload into db
link to Paper_Author_Affiliations, Authors, NSF_Investigator in R file: test_sciscinet_data.R in same folder


Create table in database:
- scinet_links_nsf

SciSciNet_Link_NSF schema is:

GrantID TEXT, PaperID INTEGER, Type TEXT
 

unique index on Grantid and PaperID (multiple PaperIDs per GrantID)
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
import sys
import requests

sys.path.append('/home/mona/mag_sample/src/dataprep/')  

from helpers.variables import db_file, datapath, databasepath
from helpers.functions import analyze_db 

scinet_path = os.path.join(datapath, "sciscinet_data/")
filepath_nsf = os.path.join(scinet_path, "SciSciNet_Link_NSF.tsv")



# Download file 

url_nsf = "https://ndownloader.figstatic.com/files/36139242"
response = requests.get(url_nsf)
with open(filepath_nsf, "wb") as file:
    file.write(response.content)
print("Downloaded data")   


# ## Read files in loop and dump to db

def load_scinet(filepath):
    df = pd.read_csv(filepath, 
                        sep="\t",
                        names=["NSF_Award_Number", "PaperID", "Type", "Diff_ZScore"], 
                        skiprows=1)
    df.drop_duplicates(inplace=True)

    # Create the GrantID column by removing non-numeric characters and formatting
    
    df['GrantID'] = df['NSF_Award_Number'].str.extract(r'-(\d+)') 
    df = df.drop(columns=['NSF_Award_Number', 'Diff_ZScore'])
    
    return df

files = [f for f in listdir(scinet_path) if isfile(join(scinet_path, f))]


con = sqlite.connect(database = db_file, isolation_level= None)
with con: 
    for (i,f) in enumerate(files):
        df = load_scinet(scinet_path+f)
        #print(df.head())
        if i==0:
            if_exists_opt="replace"
        else:
            if_exists_opt="append"

        df.to_sql("scinet_links_nsf", 
                        con=con, 
                        if_exists=if_exists_opt, 
                        index=False, 
                        schema= """ PaperID INTEGER
                                    , Type TEXT
                                    , GrantID TEXT
                                """ 
                    )

    # Make index and clean up
    con.execute("CREATE UNIQUE INDEX idx_scinet_grantpaper ON scinet_links_nsf (GrantID ASC, PaperID ASC)")

    analyze_db(con)

con.close()
