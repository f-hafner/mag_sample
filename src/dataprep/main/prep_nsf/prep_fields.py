#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script prep_fields.py

Generate tables:
- fieldsauthor: information for linking authors to nsf based on predicted field
    - AuthorId 
    - ParentFieldOfStudyId
    - authorfield_year: year // field 
- fieldsnsf: nsf fields based on abstracts (still in separate file: prep_fields_nsf.py)
    - GrantID 
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

## Predicted fields by AuthorId:    

con.execute("DROP TABLE IF EXISTS fieldsauthor")
con.execute(f"""                      
CREATE TABLE fieldsauthor AS
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
"""
)

## Predicted nsf fields by GrantID:
## To be added from prep_fields_nsf.py


# ## Run ANALYZE, finish
analyze_db(con)

con.close()