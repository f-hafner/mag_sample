#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script prep_fields_nsf.py 

Generate tables:
- fields0_nsf: information for linking authors to nsf based on predicted field on level 0
    - grantid 
    - ParentFieldOfStudyId
    - nsffield0_year: year // field0
- fields1_nsf: information for linking authors to nsf based on predicted field on level 1
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

con.execute("DROP TABLE IF EXISTS fields0_nsf")
con.execute(f"""                      
CREATE TABLE fields0_nsf AS
SELECT GrantID, ParentFieldOfStudyId
    , GROUP_CONCAT(year || "//" || fieldname, ";") AS nsffield0_year
FROM (
    SELECT DISTINCT
        c.grantid AS GrantID,
        d.year,
        a.ParentFieldOfStudyId,
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
    ) AS c ON a.ChildFieldOfStudyId = c.FieldOfStudyId
    INNER JOIN (
        SELECT GrantID 
            , CAST(SUBSTR(Award_AwardEffectiveDate, 7, 4) AS INT) AS year
        FROM NSF_MAIN {query_limit}
    ) AS d ON c.grantid = d.GrantID
    WHERE a. ParentLevel = 0
)
GROUP BY GrantID                         
"""
)

## Level 1
con.execute("DROP TABLE IF EXISTS fields1_nsf")
con.execute(f"""                      
CREATE TABLE fields1_nsf AS
SELECT GrantID, ParentFieldOfStudyId
    , GROUP_CONCAT(year || "//" || fieldname, ";") AS nsffield1_year
FROM (
    SELECT DISTINCT
        c.grantid AS GrantID,
        d.year,
        a.ParentFieldOfStudyId,
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
    ) AS c ON a.ChildFieldOfStudyId = c.FieldOfStudyId
    INNER JOIN (
        SELECT GrantID 
            , CAST(SUBSTR(Award_AwardEffectiveDate, 7, 4) AS INT) AS year
        FROM NSF_MAIN {query_limit}
    ) AS d ON c.grantid = d.GrantID
    WHERE a. ParentLevel = 1
)
GROUP BY GrantID                         
"""
)


# ## Run ANALYZE, finish
analyze_db(con)

con.close()