#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Script prep_collab.py

Generate tables:
- author_collab: a table with each unique co-author pairs for each year 

"""

import sqlite3 as sqlite
import warnings
import time 
import argparse
# import os 
# import sys 

# print(os.getcwd())
# print(os.path.isdir("helpers/"))
# print(sys.path)
# print(sys.argv)

from helpers.functions import print_elapsed_time, analyze_db
from helpers.variables import db_file 

# ## Arguments
#parser = argparse.ArgumentParser()


# ## Variables; connect to db
start_time = time.time()
print(f"Start time: {start_time} \n")

con = sqlite.connect(database = db_file, isolation_level= None)

con.execute("DROP TABLE IF EXISTS author_collab")
con.execute("""CREATE TABLE author_collab AS 
    SELECT DISTINCT AuthorId, CoAuthorId, Year 
    FROM (
        SELECT a.AuthorId, d.AuthorId AS CoAuthorId, d.Year 
        FROM PaperAuthorUnique AS a
        INNER JOIN (
            SELECT b.*, c.Year 
            FROM PaperAuthorUnique AS b
            INNER JOIN (
                SELECT PaperId, Year 
                FROM Papers
            ) AS c
            USING (PaperId)
        ) AS d
        ON (a.PaperId = d.PaperId and a.AuthorId != d.AuthorId)
        -- drop authors not in author_sample
        INNER JOIN (
            SELECT AuthorId
            FROM author_sample
        ) AS e ON (a.AuthorId = e.AuthorId)
        INNER JOIN (
            SELECT AuthorId
            FROM author_sample
        ) AS f on (CoAuthorId = f.AuthorId)
    ) 
""")


con.execute("CREATE UNIQUE INDEX idx_acllb_AuthorCoAuthorYear ON author_collab (AuthorId ASC, CoAuthorId ASC, Year)")
con.execute("CREATE INDEX idx_acllb_CoAuthorIdYear ON author_collab (CoAuthorId ASC, Year)")
con.execute("CREATE INDEX idx_acllb_AuthorIdYear ON author_collab (AuthorId ASC, Year)")


# ## Run ANALYZE, finish
analyze_db(con)

con.close()

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")

