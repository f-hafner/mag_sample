#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
genderize_unclear_names.py. Source gender parts of names where first name is not clearly assigned.

Timing: One request for 10 names takes around 0.5 seconds, so that 
    8.x Mio names would take roughly 5 days

"""

# ## Libraries
import argparse
import sqlite3 as sqlite
import requests

from helpers.variables import db_file, datapath

# ## Arguments & local variables
parser = argparse.ArgumentParser(description = 'Source gender from name elements that are unclear')
parser.add_argument("--outfile", type = str, default = 'gender_test.csv',
                    help="The file to which to save the output from genderize. (Default = gender_test.csv, in which case only 30 names are genderized.)")
parser.add_argument("--apikey", type = str, default = None, 
                    help = "key for genderize.io API")
args = parser.parse_args()

savefile = f"{datapath}/Genderize/{args.outfile}"
genderize_chunk_size = 10

# ## Genderize 

# ### Set up connections to db, set up db cursor
db_con = sqlite.connect(database = db_file,
                     isolation_level = None)

# ### Create temporary table with the counts per name 
create_table = """CREATE TEMPORARY TABLE UnclearNames AS 
                SELECT Name, COUNT(DISTINCT AuthorId) AS AuthorCount
                FROM AuthorNameSplits 
                WHERE Position > 0 
                    AND length(Name) > 1
                GROUP BY Name  
"""

db_con.execute(create_table)

# ### Cursor
sql_select = "SELECT Name FROM UnclearNames"

if args.apikey is None:
    sql_select = f"{sql_select} LIMIT 25"

cur = db_con.cursor()
cur.execute(sql_select)

# ### Iterate
with open(savefile, "w") as file_con:
    newline = ""
    while True:
        names = cur.fetchmany(size = genderize_chunk_size)
        if not names:
            print("Complete")
            break
        else:
            namelist = "&name[]=".join([name[0] for name in names])
            url = "https://api.genderize.io/?name[]=" +  namelist
            if args.apikey is not None:
                url = f"{url}&apikey={args.apikey}"
            req = requests.get(url)
            json_data = req.json()
            for i in json_data:
                file_con.write(f"{newline}{i['name']},{i['gender']},{i['probability']},{i['count']}")
                newline = "\n"


db_con.close()

