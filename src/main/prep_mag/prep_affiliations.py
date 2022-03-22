#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script prep_affiliations.py

Generate tables:
- AuthorAffiliation: author-year-affiliation where the author publishes the majority of the articles in that year 

"""

# TODO
# add some stats on the "concentration" per author-year? 
#   most likely much more concentrated than fields, so not doing for now

import sqlite3 as sqlite
import warnings
import time 
from helpers.functions import print_elapsed_time, analyze_db
from helpers.variables import db_file, insert_questionmark_doctypes, keep_doctypes


# ## Arguments

# ## Variables; connect to db
start_time = time.time()
print(f"Start time: {start_time} \n")

con = sqlite.connect(database = db_file, isolation_level= None)

print("Making AuthorAffiliation table ...\n")

con.execute("DROP TABLE IF EXISTS AuthorAffiliation")
con.execute(f"""CREATE TABLE AuthorAffiliation AS 
            SELECT AuthorId, AffiliationId, Year
            FROM (
                -- ## window function to calculate MaxPaperCount per author-year
                SELECT *,
                        MAX(PaperCount) OVER(PARTITION BY AuthorId, Year) AS MaxPaperCount
                FROM (
                    -- ## count all publications by author-affiliation-year
                    SELECT a.AuthorId, 
                            a.AffiliationId, 
                            c.Year, 
                            count(PaperId) AS PaperCount
                    FROM PaperAuthorAffiliations a
                    INNER JOIN (
                        SELECT AuthorId 
                        FROM author_sample 
                    ) b USING (AuthorId)
                    INNER JOIN (
                        SELECT PaperId, Year 
                        FROM Papers
                        WHERE DocType IN ({insert_questionmark_doctypes})
                    ) c USING (PaperId)
                    WHERE AffiliationId != ""  -- ## These are missing affiliations
                    GROUP BY a.AuthorId, a.AffiliationId, c.Year 
                ) 
            )   
            WHERE PaperCount = MaxPaperCount        
            """,
            (keep_doctypes)
            )
con.execute("CREATE UNIQUE INDEX idx_aa_AuthorAffilYear ON AuthorAffiliation (AuthorId ASC, AffiliationId ASC, Year)")
con.execute("CREATE INDEX idx_aa_Affil ON AuthorAffiliation (AffiliationId ASC) ")

# check multiple occurences within author-year 
query  = """
SELECT COUNT(DISTINCT AuthorId) 
FROM (
    SELECT AuthorId, AffiliationId, Year,
            COUNT(AffiliationId) OVER(PARTITION BY AuthorId, Year) as AffiliationCount
    FROM AuthorAffiliation  
)
WHERE AffiliationCount > 1
"""

cur = con.cursor()
cur.execute(query)

n_entries = cur.fetchall()
n_entries = n_entries[0][0]
print(f"IMPORTANT NOTE: There are {n_entries} entries in the AuthorAffiliation table with at least 2 affiliations per author-year.")

# ## Run ANALYZE, finish
analyze_db(con)

con.close()

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")

