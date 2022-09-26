#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script authors_field0.py

Extract all the field0 that the author publishes in the career

In long run, this should be combined with the code in prep_authors.py
"""

import sqlite3 as sqlite
import warnings
import time 
import argparse
from helpers.functions import print_elapsed_time, analyze_db
from helpers.variables import db_file


# ## Arguments
# parser = argparse.ArgumentParser()
# parser.add_argument("--years_first_field", type = int, default = 5,
#                     help="How many years to consider when calculating the first field of the author?")
# parser.add_argument("--years_last_field", type = int, default = 5,
#                     help="How many years to consider when calculating the last field of the author?")
# args = parser.parse_args()

# ## Variables; connect to db
start_time = time.time()
print(f"Start time: {start_time} \n")
interactive = False

# for quick runs interactively 
author_condition = ""
if interactive:
    author_condition = "WHERE AuthorId IN (584, 6193)"


con = sqlite.connect(database = db_file, isolation_level= None)

# ## Mapping of field 1 and 2 to their parents/grandparents level 0
print("Getting field parents and grandparents...", flush=True)

with con:
    con.execute("DROP TABLE IF EXISTS parent_children")

    con.execute("""
    CREATE TEMP TABLE parent_children AS 
    SELECT ChildFieldOfStudyId
        , ParentFieldOfStudyId
        , names.normalizedname, names.level
    FROM FieldsOfStudy fields
    INNER JOIN (
        SELECT FieldOfStudyId AS ParentFieldOfStudyId
            , ChildFieldOfStudyId
        FROM FieldOfStudyChildren
    ) generation0 ON(fields.FieldOfStudyId = generation0.ParentFieldOfStudyId)
    INNER JOIN (
        SELECT FieldOfStudyId, NormalizedName, Level
        FROM FieldsOfStudy
    ) names ON(generation0.ChildFieldOfStudyId = names.FieldOfStudyId)
    WHERE fields.Level = 0
    """)

    con.execute("DROP TABLE IF EXISTS grandparent_children")
    con.execute("""
    CREATE TEMP TABLE grandparent_children AS 
    SELECT generation1.ChildFieldOfStudyId
        , parent_children.ParentFieldOfStudyId AS GrandParentFieldOfStudyId
        , names.normalizedname, names.level
    FROM parent_children
    INNER JOIN (
        SELECT FieldOfStudyId, ChildFieldOfStudyId
        FROM FieldOfStudyChildren
    ) generation1 ON (parent_children.ChildFieldOfStudyId = generation1.FieldOfStudyId)
    INNER JOIN (
        SELECT FieldOfStudyId, NormalizedName, Level
        FROM FieldsOfStudy
    ) names ON(generation1.ChildFieldOfStudyId = names.FieldOfStudyId)
    """)

    # check: all grand parents should be at level 0
    # select distinct b.level from grandparent_children a inner join (select fieldofstudyid, level from fieldsofstudy) b  on(grandparentfieldofstudyid = fieldofstudyid);

    # Union; add also level 0
    con.execute("DROP TABLE IF EXISTS children_and_grandchildren_of_level0")
    con.execute("""
    CREATE TEMP TABLE children_and_grandchildren_of_level0 AS 
    SELECT ParentFieldOfStudyId as FieldOfStudyId_lvl0
        , ChildFieldOfStudyId
        , 1 as Degree
    FROM parent_children
    UNION
    SELECT GrandParentFieldOfStudyId as FieldOfStudyId_lvl0
        , ChildFieldOfStudyId
        , 2 as Degree
    FROM grandparent_children
    UNION 
    SELECT *
    FROM (
        SELECT FieldOfStudyId as FieldOfStudyId_lvl0
            , FieldOfStudyId as ChildFieldOfStudyId
            , 0 as Degree
        FROM FieldsOfStudy
        WHERE level = 0
    )
    """)

# ## Extacting the field level0 of each author
print("Getting fields level0 of authors...", flush=True)
print_elapsed_time(start_time)

with con:
    con.execute("DROP TABLE IF EXISTS author_field0")
    con.execute(f"""
    CREATE TABLE author_field0 AS
    SELECT DISTINCT AuthorId
        , FieldOfStudyId_lvl0
        , Degree 
    FROM (
        SELECT AuthorId
                , FieldOfStudyId_lvl0
                , Degree
                , MIN(Degree) OVER (PARTITION BY AuthorId, FieldOfStudyId_lvl0) as min_degree
        FROM PaperFieldsOfStudy 
        INNER JOIN (
            SELECT AuthorId, PaperId
            FROM PaperAuthorUnique
        ) USING(PaperId)
        INNER JOIN (
            SELECT AuthorId
            FROM author_sample
            {author_condition}
        ) USING(AuthorId)
        INNER JOIN (
            SELECT FieldOfStudyId
            FROM FieldsOfStudy
            WHERE Level IN (0, 1, 2)
        ) USING(FieldOfStudyId)
        INNER JOIN (
            SELECT *
            FROM children_and_grandchildren_of_level0
        ) cg ON (PaperFieldsOfStudy.FieldOfStudyId = cg.ChildFieldOfStudyId)
    )   
    WHERE Degree = min_degree
    """)

    con.execute("CREATE UNIQUE INDEX idx_af0_authorfield ON author_field0 (AuthorId ASC, FieldOfStudyId_lvl0 ASC) ")
    con.execute("CREATE INDEX idx_af0_field ON author_field0 (FieldOfStudyId_lvl0 ASC) ")


# ## Run ANALYZE, finish
with con:
    analyze_db(con)

con.close()

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")

