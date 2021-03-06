#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script author_info_linking.py

Generate tables:
- author_info_linking: information for linking authors to proquest 
    - authorid 
    - coauthors
    - keywords / fields 
    - institutions
"""

import sqlite3 as sqlite
import warnings
import time 
import argparse
from helpers.functions import print_elapsed_time, analyze_db
from helpers.variables import db_file, insert_questionmark_doctypes_citations, keep_doctypes_citations


# ## Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--years_first_field", type = int, default = 5,
                    help="How many years to consider when calculating the first field of the author?")
args = parser.parse_args()

# ## Variables; connect to db
start_time = time.time()
print(f"Start time: {start_time} \n")

con = sqlite.connect(database = db_file, isolation_level= None)

# ## Temp table with papers in first x years 
print(f"Making temp table with papers in first {args.years_first_field} years.")
print(f"--Considering papers with DocType {keep_doctypes_citations}.")
con.execute(f"""
CREATE TEMP TABLE papers_start AS 
SELECT AuthorId, PaperId 
FROM (
    SELECT a.AuthorId
        , a.PaperId
        , row_number() OVER (PARTITION BY AuthorId ORDER BY b.Date ASC)
            AS paper_number
    FROM PaperAuthorUnique AS a
    INNER JOIN (
        SELECT PaperId, Year, Date 
        FROM Papers
        WHERE DocType IN ({insert_questionmark_doctypes_citations}) -- ## NOTE: for testing, put `LIMIT 10` in next line
    ) AS b USING(PaperId)
    INNER JOIN (
        SELECT AuthorId, YearFirstPub
        FROM author_sample
    ) AS c USING(AuthorId) 
    WHERE b.Year <= c.YearFirstPub + (?)
)
WHERE paper_number <= 5 -- selects the first 5 papers if there are more from a given author
""",
(keep_doctypes_citations + (args.years_first_field,) )
)

con.execute("CREATE UNIQUE INDEX idx_ps_PaperAuthorId ON papers_start (PaperId ASC, AuthorId ASC)")
con.execute("CREATE INDEX idx_ps_AuthorId ON papers_start (AuthorId ASC) ")


# ## Create temporary tables with the variables 

# ### keywords
print_elapsed_time(start_time)
print("Creating temp table for keywords at start", flush=True)
con.execute("DROP TABLE IF EXISTS keywords")
con.execute("""
CREATE TEMP TABLE keywords AS 
SELECT AuthorId, GROUP_CONCAT(fields, ";") AS keywords
FROM (
    SELECT * 
    FROM (
        SELECT DISTINCT a.AuthorId, c.NormalizedName AS fields
        FROM papers_start a
        INNER JOIN (
            SELECT PaperId, FieldOfStudyId
            FROM PaperFieldsOfStudy
        ) b USING(PaperId)
        INNER JOIN (
            SELECT FieldOfStudyId, NormalizedName
            FROM FieldsOfStudy 
            WHERE Level IN (1)
        ) c USING(FieldOfStudyId)
    )
    ORDER BY fields
)
GROUP BY AuthorId 
""")

con.execute("CREATE UNIQUE INDEX idx_kw_AuthorId ON keywords (AuthorId ASC)")


# ### Institutions
print("Creating temp table for affiliation names at start", flush=True)
con.execute("DROP TABLE IF EXISTS institutions")
con.execute("""
CREATE TEMP TABLE institutions AS 
SELECT AuthorId, GROUP_CONCAT(institutions, ";") AS institutions
FROM (
    SELECT * 
    FROM (
        SELECT DISTINCT a.AuthorId, c.NormalizedName AS institutions
        FROM PaperAuthorAffiliations a
        INNER JOIN papers_start b USING(Paperid, AuthorId)
        INNER JOIN (
            SELECT AffiliationId, NormalizedName
            FROM Affiliations
        ) c USING(AffiliationId)
    )
    ORDER by institutions
)
GROUP BY AuthorId 
""")

con.execute("CREATE UNIQUE INDEX idx_inst_AuthorId ON institutions (AuthorId ASC)")

# ### Institutions over the whole career (for advisor linking)
print("Creating temp table for affiliation names over the whole career", flush=True)
con.execute("DROP TABLE IF EXISTS institutions_career")
con.execute(f"""
CREATE TEMP TABLE institutions_career AS 
SELECT AuthorId, GROUP_CONCAT(institutions, ";") AS institutions
FROM (
    SELECT * 
    FROM (
        SELECT DISTINCT a.AuthorId, c.NormalizedName AS institutions
        FROM PaperAuthorAffiliations a
        INNER JOIN (
            SELECT PaperId, Year, Date 
            FROM Papers
            WHERE DocType IN ({insert_questionmark_doctypes_citations}) -- ## NOTE: for testing, put `LIMIT 10` in next line
        ) AS b USING(PaperId)
        INNER JOIN (
            SELECT AuthorId, YearFirstPub
            FROM author_sample
        ) AS c USING(AuthorId) 
        INNER JOIN (
            SELECT AffiliationId, NormalizedName
            FROM Affiliations
        ) c USING(AffiliationId)
    )
    ORDER by institutions
)
GROUP BY AuthorId 
""",
keep_doctypes_citations
)

con.execute("CREATE UNIQUE INDEX idx_inst_career_AuthorId ON institutions_career (AuthorId ASC)")

# ### Co-authors
print("Creating temp tables for coauthors at start", flush=True)

con.execute("DROP TABLE IF EXISTS coauthors")
con.execute("""
CREATE TEMP TABLE coauthors AS 
SELECT AuthorId, GROUP_CONCAT(coauthors, ";") AS coauthors
FROM (
    SELECT * 
    FROM (
        SELECT DISTINCT a.AuthorId, c.NormalizedName AS coauthors
        FROM papers_start a
        INNER JOIN (
            SELECT PaperId, AuthorId as CoAuthorId
            FROM PaperAuthorUnique
        ) b USING(PaperId)
        INNER JOIN (
            SELECT AuthorId, NormalizedName
            FROM authors
        ) c ON(b.CoAuthorId = c.AuthorId)
        WHERE b.CoAuthorId != a.AuthorId
    )
    ORDER BY coauthors
)
GROUP BY AuthorId 
""")

con.execute("CREATE UNIQUE INDEX idx_coauth_AuthorId ON coauthors (AuthorId ASC)")

# ## main table 
print_elapsed_time(start_time)
print("Creating table author_info_linking", flush=True)

con.execute("DROP TABLE IF EXISTS author_info_linking")

# NOTE: use left join because some authors may not have an early career institution but have some over the course of the career 
con.execute("""
CREATE TABLE author_info_linking AS
SELECT a.AuthorId, b.keywords, c.institutions, d.coauthors, e.institutions as institutions_career
FROM author_sample a
LEFT JOIN keywords b USING(AuthorId)
LEFT JOIN institutions c USING(AuthorId)
LEFT JOIN coauthors d USING(AuthorId)
LEFT JOIN institutions_career e USING(AuthorId)
""")

con.execute("CREATE UNIQUE INDEX idx_ail_AuthorId ON author_info_linking (AuthorId ASC)")


# ## Run ANALYZE, finish
analyze_db(con)

con.close()

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")