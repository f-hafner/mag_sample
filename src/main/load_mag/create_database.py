#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Create database from MAG raw files

*Notes*
- had to create unique index names. the idea is to use idx_t_c, where t = lower(all capital letters in table name) and 
c = column name of the column used
- this does not work for indices that are on two columns... not sure what is a better system 
- need to remove "Microsoft Carriage Returns" '\r' at the end of files; 
  otherwise they show up in the last column of tables where the last column is a string. do so by copying the original file
  to the working directory as tempfile, removing '\r', loading into sqlite, and then deleting
"""

# ## Packages 
# ### general  
import subprocess
import argparse
import sqlite3 as sqlite

# ### helpers 
from helpers.MAGTableDefinitions import MAGtables_setup
from helpers.variables import db_file, databasepath, rawdatapath
from helpers.functions import analyze_db

# ## Arguments
parser = argparse.ArgumentParser(description = 'Inputs creating sqlite database')
parser.add_argument("--nlines", type = int,
                    help="Number of lines to be read into database from each raw file.")
args = parser.parse_args()

# ## connect to db
con = sqlite.connect(database = db_file, isolation_level= None)


# ## Helper functions
def copy_trim(filename, nlines = args.nlines):
    """
    Copy `filename` from `rawdatapath` to `databasepath` and save as `tempfile.txt`. Can then be read into db.
    """
    if nlines:
        cat_file = f"head -n {nlines} {rawdatapath}{filename}"
    else:
        cat_file = f"cat {rawdatapath}{filename}"
    
    trim_file = "tr -d $'\r'"
    save_file = f"{databasepath}tempfile.txt"
    cmd = f"{cat_file} | {trim_file} > {save_file}"
    subprocess.run(cmd, shell = True)

def remove_file(filename):
    subprocess.run(f"rm -rf {databasepath}{filename}", shell = True)

def file_to_table(filename, tablename):
    """
    Read in `filename` into table `tablename`. 
        - Defines separator for fields and lines.
        - Uses the sqlite `.import` utility which is quite fast.
    """
    subprocess.run(
        ["sqlite3", db_file,
        ".mode ascii", 
        ".separator \"\\t\" \"\\n\" ",
        f".import {databasepath}{filename} {tablename}"
        ]
    )


# ## Loop over MAGtables_setup and read in 
for tbl in MAGtables_setup.keys():
    print(f"Reading {tbl} \n")
    # prepare file
    copy_trim(MAGtables_setup[tbl]['rawfile'])

    # read table
    con.execute(f'DROP TABLE IF EXISTS {tbl}')
    con.execute(MAGtables_setup[tbl]['sql_create_table'])
    file_to_table('tempfile.txt', tbl)
    if MAGtables_setup[tbl]['sql_create_index'] is not None:
        con.execute(MAGtables_setup[tbl]['sql_create_index'])

    # clean up 
    remove_file('tempfile.txt')


# ## Table with first names
print("Unique first names \n")
con.execute("DROP TABLE IF EXISTS FirstNames")
con.execute("""CREATE TABLE FirstNames AS 
            SELECT 
                SUBSTR(TRIM(NormalizedName),1,instr(trim(NormalizedName)||' ',' ')-1) AS FirstName,
                COUNT(DISTINCT AuthorId) AS AuthorCount 
            FROM Authors
            WHERE length(FirstName) > 1 
            GROUP BY FirstName 
            """)
con.execute("CREATE INDEX idx_fn_FirstName ON FirstNames (FirstName)")

# ## Some additional indexes for faster queries (not in original MAG)
con.execute("CREATE INDEX IF NOT EXISTS idx_p_Year ON Papers (Year ASC) ")
con.execute("CREATE INDEX IF NOT EXISTS idx_p_DocType ON Papers (DocType) ") 
con.execute("CREATE INDEX IF NOT EXISTS idx_pr_PaperReferenceId ON PaperReferences (PaperReferenceId ASC)")


# ## Run ANALYZE
analyze_db(con)


# ## Show output, finish
con.close()

print(f'Schema: \n')
subprocess.run(
    ["sqlite3", db_file,
    ".schema",
    ]
)

print('Done.')


