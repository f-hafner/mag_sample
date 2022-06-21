#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script affiliation_outcomes.py

Generate tables:
- affiliation_outcomes: some features at the affiliation level.
    - number of journal articles published per affiliation. In contrast to table Affiliations,
        consider only keep_doctypes
NOTE: in the long run, we may consider to move this to prep_affiliations.py, or unify them in a new file.
"""

# TODO
# add some stats on the "concentration" per author-year? 
#   most likely much more concentrated than fields, so not doing for now

import sqlite3 as sqlite
import warnings
import time 
from helpers.functions import analyze_db
from helpers.variables import db_file, insert_questionmark_doctypes, keep_doctypes


# ## Arguments

# ## Variables; connect to db
start_time = time.time()
print(f"Start time: {start_time} \n")

con = sqlite.connect(database = db_file, isolation_level= None)

print("Making affiliation_outcomes table ...\n")

con.execute("DROP TABLE IF EXISTS affiliation_outcomes")
con.execute(f"""CREATE TABLE affiliation_outcomes AS 
            SELECT AffiliationId, COUNT(DISTINCT PaperId) AS PublicationCount
            from PaperAuthorAffiliations
            INNER JOIN (
                SELECT PaperId
                FROM Papers 
                WHERE DocType IN ({insert_questionmark_doctypes})
                    AND Year >= 1950
            ) USING(PaperId)
            INNER JOIN (
                SELECT AffiliationId 
                FROM Affiliations
            ) USING (AffiliationId)
            GROUP BY (AffiliationId)   
            """,
            (keep_doctypes)
            )
con.execute("CREATE UNIQUE INDEX idx_affo_AffiliationId ON affiliation_outcomes (AffiliationId ASC)")


# ## Run ANALYZE, finish
analyze_db(con)

con.close()

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")

