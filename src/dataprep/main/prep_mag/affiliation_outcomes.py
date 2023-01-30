#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script affiliation_outcomes.py

Generate tables: at the affiliation-year-field0 level
- affiliation_outcomes: publication outcomes
    - number of journal articles published and 10-year forward citations per 
        affiliation-year-field0. Field0 is assigned from the Field0 of the published paper.
    - number of authors publishing in that field and having one of their main affiliations in that year at that institution
- affiliation_fields: keywords of published papers 
    - fields of study of the published paper 
NOTE: in the long run, we may consider to move this to prep_affiliations.py, or unify them in a new file.
"""

import argparse 
import sqlite3 as sqlite
import warnings
import time 
from helpers.functions import analyze_db
from helpers.variables import db_file


# ## Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--fos_max_level", type=int, default=2,
                    help="Fields of study up to which level to include?")
args = parser.parse_args()

# ## Variables; connect to db
start_time = time.time()
print(f"Start time: {start_time} \n")
interactive = False # Turn this on for only querying few records, ie for testing


con = sqlite.connect(database = db_file, isolation_level= None)

print("Creating temp tables paper_affiliation_year and author_affiliation_year", flush=True)

query_limit = ""
if interactive:
    query_limit = "LIMIT 1000"


base_query = f""" 
SELECT a.AuthorId, a.AffiliationId, a.Year, b.Paperid
FROM AuthorAffiliation a -- ## if an author has 2 main affiliations in the same year, we count their papers at both institutions
INNER JOIN (
    SELECT PaperId, AuthorId, Year
    FROM PaperAuthorUnique
    INNER JOIN (
        SELECT PaperId, Year
        FROM Papers
    ) USING(PaperId)
    {query_limit}
) b
ON a.AuthorId=b.AuthorId AND a.Year=b.Year
-- reduces size of the data set 
INNER JOIN (
    SELECT PaperId
    FROM paper_outcomes
) USING(PaperId)
"""

with con as c:
    c.execute(f"""
        CREATE TEMP TABLE paper_affiliation_year AS 
        SELECT DISTINCT AffiliationId, Year, PaperId
        FROM (
            {base_query}
        )
    """)

    c.execute("CREATE INDEX idx_paper_temp ON paper_affiliation_year (PaperId)")

    c.execute(f"""
        CREATE TEMP TABLE author_affiliation_year AS 
        SELECT DISTINCT AuthorId, AffiliationId, Year, PaperId
        FROM (
            {base_query}
        )
    """)

    c.execute("CREATE INDEX idx_paper_aay_temp ON author_affiliation_year (PaperId)")
    c.execute("CREATE INDEX idx_author_aay_temp ON author_affiliation_year (AuthorId)")

print("Creating tables affiliation_outcomes_temp and author_affiliation_field")
with con as c:
    c.execute("""
        CREATE TEMP TABLE affiliation_outcomes_temp AS  
        SELECT AffiliationId
            , Year
            , Field0
            , COUNT(PaperId) AS PaperCount
            , SUM(CitationCount_y10) AS CitationCount_y10
        FROM paper_affiliation_year 
        INNER JOIN (
            SELECT PaperId, CitationCount_y10 
            FROM paper_outcomes 
        ) USING(PaperId)
        INNER JOIN ( 
            SELECT PaperId, Field0 
            FROM PaperMainFieldsOfStudy
        ) 
        USING(PaperId)
        GROUP BY AffiliationId, Year, Field0
    """)

    c.execute("CREATE UNIQUE INDEX idx_affo_temp_AffilYearField ON affiliation_outcomes_temp (AffiliationId, Year, Field0)")

    c.execute("""
        CREATE TEMP TABLE author_affiliation_field AS 
        SELECT AffiliationId
            , Year
            , Field0
            , COUNT(DISTINCT AuthorId) AS AuthorCount
        FROM author_affiliation_year
        INNER JOIN ( 
            SELECT PaperId, Field0 
            FROM PaperMainFieldsOfStudy
        ) 
        USING(PaperId)
        GROUP BY AffiliationId, Year, Field0
    """)

    c.execute("CREATE UNIQUE INDEX idx_aaf_AffilYearField ON author_affiliation_field (AffiliationId, Year, Field0)")


print("Creating table affiliation_outcomes", flush=True)
with con as c:
    c.execute("DROP TABLE IF EXISTS affiliation_outcomes")
    c.execute("""
        CREATE TABLE affiliation_outcomes AS 
        SELECT a.*, b.AuthorCount
        FROM affiliation_outcomes_temp AS a
        INNER JOIN 
        author_affiliation_field AS b
        ON a.AffiliationId=b.AffiliationId
            AND a.Year=b.Year
            AND a.Field0=b.Field0
    """)

    c.execute("CREATE UNIQUE INDEX idx_affo_AffilYearField ON affiliation_outcomes (AffiliationId, Year, Field0)")


print("Creating table affiliation_fields", flush=True)

with con as c:
    c.execute("DROP TABLE IF EXISTS affiliation_fields")

    c.execute(f"""
        CREATE TABLE affiliation_fields AS 
        SELECT AffiliationId
            , Field0
            , Year 
            , FieldOfStudyId
            , SUM(Score) AS Score
        FROM paper_affiliation_year 
        INNER JOIN (
            SELECT PaperId, FieldOfStudyId, Score
            FROM PaperFieldsOfStudy 
            INNER JOIN (
                SELECT FieldOfStudyId 
                FROM FieldsOfStudy 
                WHERE level >= {args.fos_max_level} 
            ) USING(FieldOfStudyId)
        ) USING(PaperId)
        INNER JOIN ( 
            SELECT PaperId, Field0 
            FROM PaperMainFieldsOfStudy
        ) USING(PaperId)
        GROUP BY AffiliationId, FieldOfStudyId, Year, Field0
    """)

    c.execute("CREATE UNIQUE INDEX idx_afff_AffilFieldYearField ON affiliation_fields (AffiliationId, FieldOfStudyId, Year, Field0)")
    c.execute("CREATE INDEX idx_afff_Year ON affiliation_fields (Year)")
    c.execute("CREATE INDEX idx_afff_FoS ON affiliation_fields (FieldOfStudyId)")



# ## Run ANALYZE, finish
with con as c:
    analyze_db(c)


con.close()

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")

