#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Check that tables in database are the same as the original csv files.

"""

import os
import warnings
import sys

import sqlite3 as sqlite
import subprocess
import pdb
import re

from helpers.variables import rawdatapath, db_file, mag_file_locations as file_locations

conn = sqlite.connect(database = db_file)

db_tables = conn.execute("SELECT NAME FROM SQLITE_MASTER WHERE TYPE = 'table' ").fetchall()

print('Comparing number of records in each table with number of lines in original files... \n')

for table_tuple in db_tables:
  table = table_tuple[0]
  if table in file_locations.keys():
    original_file = "%s%s%s.txt" % (rawdatapath, file_locations[table], table)
    
    nrow_db = conn.execute('SELECT COUNT(*) FROM %s LIMIT 10' % (table)).fetchone()[0]
    nrow_file = int(subprocess.check_output('wc -l %s' % (original_file), shell = True).split()[0])
    
    print(table)
    print('Number of records and lines match: %s \n' % (nrow_db == nrow_file))


conn.close()
print('Done.')  






