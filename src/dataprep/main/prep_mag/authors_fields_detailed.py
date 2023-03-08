#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script authors_fields_detailed.py

Generate table:
- authors_fields_detailed: score and number of papers per authorid, year, fieldofstudyid
- eventually, this could be combined with prep_authors.py, and the script that creates author_output 
    (since we are calculating number of papers again here)
"""

import sqlite3 as sqlite
import warnings
import time 
import argparse
import logging 
from helpers.functions import print_elapsed_time, analyze_db
from helpers.variables import db_file, insert_questionmark_doctypes, keep_doctypes

logging.basicConfig(level=logging.INFO)

# ## Arguments
parser = argparse.ArgumentParser()
args = parser.parse_args()

# ## Variables; connect to db
start_time = time.time()
print(f"Start time: {start_time} \n")
interactive = False

con = sqlite.connect(database = db_file, isolation_level= None)

query_limit = ""
if interactive:
    query_limit = "LIMIT 1000"

base_query = f"""
    SELECT PaperId
        , AuthorId
        , FieldOfStudyId
        , Score
        , Year
    FROM PaperAuthorUnique
    INNER JOIN (
        SELECT AuthorId 
        FROM author_sample
    ) USING(AuthorId)
    INNER JOIN (
        SELECT PaperId
            , FieldOfStudyId
            , Score
        FROM PaperFieldsOfStudy
    ) USING(PaperId)
    INNER JOIN (
        SELECT PaperId, Year 
        FROM Papers
        WHERE DocType IN ({insert_questionmark_doctypes})
    ) USING(PaperId)
    {query_limit}
"""


with con as c:
    logging.debug("Running base query")
    c.execute(f"""
        CREATE TEMP TABLE author_paper_field_temp AS 
        {base_query}
        """, 
        keep_doctypes
    )

    logging.debug("Creating temp tables")
    c.execute("""
    CREATE TEMP TABLE author_field_temp AS
    SELECT AuthorId
        , FieldOfStudyId
        , Year 
        , SUM(Score) AS Score 
    FROM author_paper_field_temp
    GROUP BY AuthorId, FieldOfStudyId, Year 
    """)
    
    c.execute("CREATE UNIQUE INDEX idx_tmp1 ON author_field_temp (AuthorId ASC, Year ASC, FieldOfStudyId)")

    c.execute("""
    CREATE TEMP TABLE author_paper_count AS
    SELECT AuthorId
        , Year
        , COUNT(DISTINCT PaperId) AS PaperCount
    FROM author_paper_field_temp
    GROUP BY AuthorId, Year
    """)

    c.execute("CREATE UNIQUE INDEX idx_tmp2 ON author_paper_count (AuthorId ASC, Year ASC)")

    logging.debug("Making final table")
    c.execute("DROP TABLE IF EXISTS author_fields_detailed")

    c.execute("""
    CREATE TABLE author_fields_detailed AS 
    SELECT * 
    FROM author_field_temp
    INNER JOIN author_paper_count
    USING(AuthorId, Year)
    """)

    indexes = [
        "UNIQUE INDEX idx_afd_autfieldyr ON author_fields_detailed (AuthorId ASC, FieldOfStudyId, Year ASC)",
        "INDEX idx_adf_field ON author_fields_detailed (FieldOfStudyId ASC)",
        "INDEX idx_adf_yr ON author_fields_detailed (YEAR ASC)"
    ]

    for idx in indexes:
        c.execute(f"CREATE {idx}")


# ## Run ANALYZE, finish
with con as c:
    analyze_db(c)


con.close()

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")



