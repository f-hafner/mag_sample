#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script author_info_linking.py

Generate tables:
- author_info_linking: information for linking authors to proquest 
    - authorid 
    - coauthors
    - keywords / fields 
    - institutions:
        - main_institutions_career: list of institutions 
           with whose affiliation the author ever published 
           a document in keep_doctypes_citations during the career 
        - main_us_institutions_career: same as above, but only
           US-institutions
        - main_us_institutions_year: unique pairs of year and 
           institution name from AuthorAffiliation
        - all_us_institutions_year: unique pairs of year and 
           institution name from *all* documents ever 
           published by the author
    - year and title of papers in the first years of the career
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

interactive = False

# ## Variables; connect to db
start_time = time.time()
print(f"Start time: {start_time} \n", flush=True)

con = sqlite.connect(database = db_file, isolation_level= None)

query_limit = "" 
if interactive:
    # shorten query limits for faster runs when trying out code. 
    query_limit = "LIMIT 10" # make sure all query have this somewhere!

# ## Temp table with papers in first x years 
print(f"Making temp table with papers in first {args.years_first_field} years.", flush=True)
print(f"--Considering papers with DocType {keep_doctypes_citations}.", flush=True)
con.execute(f"""
CREATE TEMP TABLE papers_start AS 
SELECT AuthorId
    , PaperId
FROM (
    SELECT a.AuthorId
        , a.PaperId
        , row_number() OVER (PARTITION BY AuthorId ORDER BY b.Date ASC)
            AS paper_number
    FROM PaperAuthorUnique AS a
    INNER JOIN (
        SELECT PaperId, Year, Date
        FROM Papers
        WHERE DocType IN ({insert_questionmark_doctypes_citations}) {query_limit} 
    ) AS b USING(PaperId)
    INNER JOIN (
        SELECT AuthorId, YearFirstPub
        FROM author_sample
    ) AS c USING(AuthorId) 
    WHERE b.Year <= c.YearFirstPub + (?)
)
WHERE paper_number <= 10 -- selects the first 10 papers if there are more from a given author
""",
(keep_doctypes_citations + (args.years_first_field,) )
)

con.execute("CREATE UNIQUE INDEX idx_ps_PaperAuthorId ON papers_start (PaperId ASC, AuthorId ASC)")
con.execute("CREATE INDEX idx_ps_AuthorId ON papers_start (AuthorId ASC) ")


# ## Create temporary tables with the variables 

# ### paper titles
print_elapsed_time(start_time)
print("Creating temp table for paper titles at start", flush=True)
con.execute("DROP TABLE IF EXISTS paper_titles_start")
con.execute("""
CREATE TEMP TABLE paper_titles_start AS 
SELECT AuthorId, GROUP_CONCAT(year_papertitle, ";") AS year_papertitle
FROM (
    SELECT AuthorId
        , Year || "//" || PaperTitle AS year_papertitle
    FROM (
        SELECT a.AuthorId
            , b.PaperTitle
            , b.Year
            , MIN(b.Year) AS min_year -- this selects the year of the paper when it first appears 
        FROM papers_start a
        INNER JOIN (
            SELECT PaperId, Year, PaperTitle
            FROM Papers
        ) b USING(PaperId)
        -- keep only the first occurence of a paper if there are multiple versions with the same title 
        GROUP BY AuthorId, PaperTitle
    )
    WHERE Year = min_year
    ORDER BY Year
)
GROUP BY AuthorId 
""")

con.execute("CREATE UNIQUE INDEX idx_pts_AuthorId ON paper_titles_start (AuthorId ASC)")

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

# ### Main institutions over the whole career (for advisor linking)
    # main = where a paper is published
print("Creating temp table for affiliation names over the whole career", flush=True)
con.execute("DROP TABLE IF EXISTS institutions_career")
con.execute(f"""
CREATE TEMP TABLE institutions_career AS 
SELECT AuthorId
    , GROUP_CONCAT(main_institutions, ";") AS main_institutions
    , GROUP_CONCAT(main_us_institutions, ";") AS main_us_institutions
FROM (
    SELECT * 
    FROM (
        SELECT DISTINCT a.AuthorId
            , c.NormalizedName AS main_institutions
            , d.US_NormalizedName AS main_us_institutions
        FROM PaperAuthorAffiliations a
        INNER JOIN (
            SELECT PaperId, Year, Date 
            FROM Papers
            WHERE DocType IN ({insert_questionmark_doctypes_citations}) {query_limit} 
        ) AS b USING(PaperId)
        INNER JOIN (
            SELECT AuthorId, YearFirstPub
            FROM author_sample
        ) AS c USING(AuthorId) 
        INNER JOIN (
            SELECT AffiliationId, NormalizedName
            FROM Affiliations
        ) c USING(AffiliationId)
        LEFT JOIN ( -- # LEFT JOIN: keep non-us institutions above 
            SELECT AffiliationId, NormalizedName AS US_NormalizedName
            FROM Affiliations
            WHERE Iso3166Code = 'US'
        ) d USING(AffiliationId)
    )
    ORDER by main_us_institutions
)
GROUP BY AuthorId 
""",
keep_doctypes_citations
)

con.execute("CREATE UNIQUE INDEX idx_inst_career_AuthorId ON institutions_career (AuthorId ASC)")


# ### combinations of year-main institution over the whole career (for grant linking)
print("Creating temp table for affiliation-year pairs over career", flush=True)
con.execute("DROP TABLE IF EXISTS institutions_year_career")
con.execute(f"""
CREATE TEMP TABLE institutions_year_career AS 
SELECT AuthorId
    , GROUP_CONCAT(us_institutions_year, ";") AS main_us_institutions_year
FROM (
    SELECT * 
    FROM (
        SELECT a.AuthorId
            , a.Year || '//' || d.normalizedname AS us_institutions_year
        FROM AuthorAffiliation a 
        INNER JOIN (
            SELECT AuthorId, YearFirstPub
            FROM author_sample {query_limit} 
        ) AS c USING(AuthorId) 
        INNER JOIN (
            SELECT from_id, normalizedname
            FROM links_to_cng
            INNER JOIN (
                SELECT unitid, normalizedname 
                FROM cng_institutions
            ) USING (unitid)
            WHERE from_dataset = "mag"
        ) d ON a.AffiliationId = d.from_id
        ORDER BY Year
    )
)
GROUP BY AuthorId 
"""
)

con.execute("CREATE UNIQUE INDEX idx_inst_career_year_AuthorId ON institutions_year_career (AuthorId ASC)")


# ### combinations of year-institution over the whole career (for grant linking)
    # uses *any* document ever published by the person
print("Creating temp table for affiliation-year pairs over career", flush=True)
con.execute("DROP TABLE IF EXISTS all_institutions_year_career")
con.execute(f"""
CREATE TEMP TABLE all_institutions_year_career AS 
SELECT AuthorId
    , GROUP_CONCAT(year || "//" || affiliation_name, ";") AS all_us_institutions_year
FROM (
    SELECT DISTINCT AuthorId, affiliation_name, Year
    FROM (
        SELECT * 
        FROM PaperAuthorAffiliations
        INNER JOIN (
            SELECT from_id, affiliation_name
            FROM links_to_cng
            INNER JOIN (
                SELECT unitid, normalizedname AS affiliation_name
                FROM cng_institutions
            ) USING (unitid)
            WHERE from_dataset = "mag"
        ) d ON AffiliationId = d.from_id
        INNER JOIN (
            SELECT PaperId, Year
            FROM Papers 
        ) USING (PaperId)
        INNER JOIN (
            SELECT AuthorId
            FROM author_sample {query_limit} 
        ) USING (AuthorId)
    )
    ORDER BY Year, affiliation_name
)
GROUP BY AuthorId
"""
)

con.execute("CREATE UNIQUE INDEX idx_allinst_career_year_AuthorId ON all_institutions_year_career (AuthorId ASC)")


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

## Create table for predicted fields of authors

con.execute("DROP TABLE IF EXISTS fieldsauthor")
con.execute(f"""                      
CREATE TEMP TABLE fieldsauthor AS
SELECT AuthorId, ParentFieldOfStudyId
    , GROUP_CONCAT(year || "//" || fieldname, ";") AS authorfield_year
FROM (
    SELECT DISTINCT
        c.AuthorId,
        c.Year,
        a.ParentFieldOfStudyId,
        b.NormalizedName AS fieldname
    FROM
        crosswalk_fields a
    INNER JOIN (
        SELECT FieldOfStudyId, NormalizedName
        FROM FieldsOfStudy 
    ) AS b ON a.ParentFieldOfStudyId = b.FieldOfStudyId
    INNER JOIN (
        SELECT AuthorId, FieldOfStudyId, Year
        FROM author_fields_detailed {query_limit}
    ) AS c ON a.ChildFieldOfStudyId = c.FieldOfStudyId
    WHERE a. ParentLevel = 0
)
GROUP BY AuthorId                         
""")

con.execute("CREATE UNIQUE INDEX idx_authfield_AuthorId ON fieldsauthor (AuthorId ASC)")


# ## main table 
print_elapsed_time(start_time)
print("Creating table author_info_linking", flush=True)

con.execute("DROP TABLE IF EXISTS author_info_linking")

# NOTE: use left join because some authors may not have an early career institution but have some over the course of the career 
con.execute("""
CREATE TABLE author_info_linking AS
SELECT a.AuthorId
    , b.keywords
    , c.institutions
    , d.coauthors
    , e.main_institutions as main_institutions_career
    , e.main_us_institutions as main_us_institutions_career
    , f.main_us_institutions_year
    , g.all_us_institutions_year
    , h.year_papertitle
    , i.authorfield_year
FROM author_sample a
LEFT JOIN keywords b USING(AuthorId)
LEFT JOIN institutions c USING(AuthorId)
LEFT JOIN coauthors d USING(AuthorId)
LEFT JOIN institutions_career e USING(AuthorId)
LEFT JOIN institutions_year_career f USING(AuthorId)
LEFT JOIN all_institutions_year_career g USING(AuthorId)
LEFT JOIN paper_titles_start h USING(AuthorId)
LEFT JOIN fieldsauthor i USING(AuthorId)
""")

con.execute("CREATE UNIQUE INDEX idx_ail_AuthorId ON author_info_linking (AuthorId ASC)")


# ## Run ANALYZE, finish
analyze_db(con)

con.close()

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")