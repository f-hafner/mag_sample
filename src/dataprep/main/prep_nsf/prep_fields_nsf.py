#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script prep_fields_nsf.py

Generate tables:
- fieldsnsf: information for linking authors to nsf based on predicted field
    - grantid 
    - ParentFieldOfStudyId
    - nsffield_year: year // field
    
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

con.execute("DROP TABLE IF EXISTS fieldsnsf")
con.execute(f"""                      
CREATE TABLE fieldsnsf AS
SELECT GrantID, ParentFieldOfStudyId
    , GROUP_CONCAT(year || "//" || fieldname, ";") AS nsffield_year
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
        FROM nsffos {query_limit}
    ) AS c ON a.ChildFieldOfStudyId = c.FieldOfStudyId
    INNER JOIN (
        SELECT GrantID 
            , CAST(SUBSTR(Award_AwardEffectiveDate, 7, 4) AS INT) AS year
        FROM NSF_MAIN
    ) AS d ON c.grantid = d.GrantID
    WHERE a. ParentLevel = 0
)
GROUP BY GrantID                         
"""
)


# ## Run ANALYZE, finish
analyze_db(con)

con.close()