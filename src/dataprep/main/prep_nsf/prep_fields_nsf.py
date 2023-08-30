#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script prep_fields_nsf.py 

Generate tables:
- nsf_fields0_collapsed: information for linking authors to nsf based on predicted field on level 0
    - grantid 
    - ParentFieldOfStudyId
    - nsffield0_year: year // field0
- nsf_fields0_collapsed: information for linking authors to nsf based on predicted field on level 1
    - grantid 
    - ParentFieldOfStudyId
    - nsffield0_year: year // field1    
"""
 
import sqlite3 as sqlite
import warnings
import time 
import argparse
import sys

sys.path.append('/home/mona/mag_sample/src/dataprep/')  
from helpers.functions import print_elapsed_time, analyze_db
from helpers.variables import db_file, insert_questionmark_doctypes_citations, keep_doctypes_citations


interactive = True

# ## Variables; connect to db
con = sqlite.connect(database = db_file, isolation_level= None)

query_limit = "" 
if interactive:
    # shorten query limits for faster runs when trying out code. 
    query_limit = "LIMIT 10" # make sure all query have this somewhere!


## Level 0:
con.execute("DROP TABLE IF EXISTS nsf_fields0_collapsed")
con.execute(f"""                      
CREATE TABLE nsf_fields0_collapsed AS
SELECT GrantID
    , GROUP_CONCAT(year || "//" || fieldname, ";") AS field0_year
FROM (
    SELECT DISTINCT
        c.grantid AS GrantID,
        d.year,
        b.NormalizedName AS fieldname
    FROM
        crosswalk_fields a
    INNER JOIN (
        SELECT FieldOfStudyId, NormalizedName
        FROM FieldsOfStudy 
    ) AS b ON a.ParentFieldOfStudyId = b.FieldOfStudyId
    INNER JOIN (
        SELECT grantid, FieldOfStudyId
        FROM nsffos 
        WHERE score > 0.4
    ) AS c ON a.ChildFieldOfStudyId = c.FieldOfStudyId
    INNER JOIN (
        SELECT GrantID 
            , CAST(SUBSTR(Award_AwardEffectiveDate, 7, 4) AS INT) AS year
        FROM NSF_MAIN {query_limit}
    ) AS d ON c.grantid = d.GrantID
    WHERE a. ParentLevel = 0
UNION 
    SELECT DISTINCT
        b.grantid AS GrantID,
        c.year,
        a.NormalizedName AS fieldname
    FROM FieldsOfStudy a
    INNER JOIN (
        SELECT grantid, FieldOfStudyId
        FROM nsffos 
        WHERE score > 0.4
    ) AS b USING(FieldOfStudyId) 
    INNER JOIN (
        SELECT GrantID 
            , CAST(SUBSTR(Award_AwardEffectiveDate, 7, 4) AS INT) AS year
        FROM NSF_MAIN {query_limit}
    ) AS c ON b.grantid = c.GrantID
    WHERE a. Level = 0
)
GROUP BY GrantID                         
""")

## Level 1
con.execute("DROP TABLE IF EXISTS nsf_fields1_collapsed")
con.execute(f"""                      
CREATE TABLE nsf_fields1_collapsed AS
SELECT GrantID
    , GROUP_CONCAT(year || "//" || fieldname, ";") AS field1_year
FROM (
    SELECT DISTINCT
        c.grantid AS GrantID,
        d.year,
        b.NormalizedName AS fieldname
    FROM
        crosswalk_fields a
    INNER JOIN (
        SELECT FieldOfStudyId, NormalizedName
        FROM FieldsOfStudy 
    ) AS b ON a.ChildFieldOfStudyId = b.FieldOfStudyId
    INNER JOIN (
        SELECT grantid, FieldOfStudyId
        FROM nsffos 
        WHERE score > 0.4
    ) AS c ON a.ChildFieldOfStudyId = c.FieldOfStudyId
    INNER JOIN (
        SELECT GrantID 
            , CAST(SUBSTR(Award_AwardEffectiveDate, 7, 4) AS INT) AS year
        FROM NSF_MAIN {query_limit}
    ) AS d ON c.grantid = d.GrantID
    WHERE a. ParentLevel = 1
UNION 
    SELECT DISTINCT
        b.grantid AS GrantID,
        c.year,
        a.NormalizedName AS fieldname
    FROM FieldsOfStudy a
    INNER JOIN (
        SELECT grantid, FieldOfStudyId
        FROM nsffos 
        WHERE score > 0.4
    ) AS b USING(FieldOfStudyId) 
    INNER JOIN (
        SELECT GrantID 
            , CAST(SUBSTR(Award_AwardEffectiveDate, 7, 4) AS INT) AS year
        FROM NSF_MAIN {query_limit}
    ) AS c ON b.grantid = c.GrantID
    WHERE a. Level = 1
)
GROUP BY GrantID                         
"""
)


# ## Run ANALYZE, finish
analyze_db(con)

con.close()