#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Load proquest dissertation attributes into database table pq_attributes.
Creates table with CSV file from src/dataprep/main/load_proquest/proquest_attributes_read.py
"""

import subprocess
import sqlite3 as sqlite
import os
import pandas as pd

from helpers.variables import db_file, datapath, databasepath
from helpers.functions import analyze_db

# Connect to database
con = sqlite.connect(database=db_file, isolation_level=None)

 
# Create temporary CSV for sqlite import
filename = datapath + "pq_attributes.csv"
# Create and populate table
con.execute("DROP TABLE IF EXISTS pq_attributes")
con.execute("""
    CREATE TABLE pq_attributes(
        goid INTEGER,
        abswordcount INTEGER,
        title TEXT
    )
""")

# Import data
subprocess.run(
    ["sqlite3", db_file,
    ".mode csv",
    f".import --csv --skip 1 {filename} pq_attributes"]
)

# Create index
con.execute("CREATE UNIQUE INDEX idx_pqattr_goid ON pq_attributes (goid ASC)")

# Clean up
#os.remove(filename)

# Analyze and finish
analyze_db(con)
con.close()

print("Done.")
