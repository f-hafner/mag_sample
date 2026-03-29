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

GrantID TEXT, PaperID INTEGER, Type TEXT, Diff_ZScore NUMERIC
 

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
#import sys
#import requests

#sys.path.append('/home/mona/mag_sample/src/dataprep/')  

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


# ## Read file and dump to db
# remove first row as it only contains column names that can't be overwritten
def load_scinet(filepath):
    df = pd.read_csv(filepath, 
                        sep="\t",
                        names=["NSF_Award_Number", "PaperID", "Type", "Diff_ZScore"], 
                        skiprows=1)
    df.drop_duplicates(inplace=True)

    # Create a GrantID variable in same format as previously used by removing non-numeric characters from NSF_Award_Number 
    # drop NSF_Award_Number as we only need GrantID
    
    df['GrantID'] = df['NSF_Award_Number'].str.extract(r'-(\d+)') 
    df = df.drop(columns=['NSF_Award_Number'])

    #Check that all rows will be uploaded into db: in raw file 1309518 rows
    num_observations = df.shape[0]
    print(num_observations, "rows of 1309518 rows in the raw file will be loaded into the db")
    
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
                                    , Diff_ZScore NUMERIC
                                """ 
                    )

    # Make index and clean up: 
    # Serves as check that only unique observations part of the dataframe
    con.execute("CREATE UNIQUE INDEX idx_scinet_grantpaper ON scinet_links_nsf (GrantID ASC, PaperID ASC)")

    analyze_db(con)

con.close()
