#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Read in the gender X first name table 
"""

import subprocess
import sqlite3 as sqlite
import argparse

from helpers.variables import db_file, datapath


parser = argparse.ArgumentParser(description = 'Read gender table')
parser.add_argument("--from_file", type = str, default = 'gender_test.csv',
                    help="The file to which to save the output from genderize. (Default = gender_test.csv)")
parser.add_argument("--to_table", type = str, default = "GenderTest",
                    help = "The name of the table to be created (default = GenderTest).")
parser.add_argument("--index", type = str, required = True,
                    help = "The index on FirstName for the resulting table")
args = parser.parse_args()

# ## Variables; connect to db
genderfile = f"{datapath}Genderize/{args.from_file}"

print(f"Reading {genderfile} to table {args.to_table} ... \n")

# ## Create table, read in
con = sqlite.connect(database = db_file, isolation_level= None)
con.execute('DROP TABLE IF EXISTS import_genderize')
con.execute("""CREATE TABLE import_genderize(
                FirstName TEXT,
                Gender TEXT DEFAULT NULL,
                Probability NUMERIC,
                PersonCount INTEGER
                )
            """)

subprocess.run(
    ["sqlite3", db_file,
    ".mode csv", 
    f".import {genderfile} import_genderize"
    ]
)

con.execute("UPDATE import_genderize SET Gender = NULL WHERE Gender = '' ")

# ## Transform: define female = {0,1} and -1 if Probability is low 
    # Huang et al drop countries where performance is bad, but do not per se 
    # drop certain names b/c of matching accuracy
con.execute(f"DROP TABLE IF EXISTS {args.to_table}")
con.execute(f"""CREATE TABLE {args.to_table} AS 
            SELECT FirstName, PersonCount,
                   CASE 
                    WHEN Gender = "male" THEN 1 - Probability
                    WHEN Gender = "female" THEN Probability 
                   END AS ProbabilityFemale
            FROM import_genderize
            """)

con.execute(f"CREATE INDEX {args.index} ON {args.to_table} (FirstName ASC)")
con.execute("DROP TABLE import_genderize")

con.close()
print('Done.')