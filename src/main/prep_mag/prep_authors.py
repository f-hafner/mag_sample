#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script prep_authors.py

Generate tables:
- PaperAuthorUnique: unique paper-author combinations
- author_sample: select authors that satisfy sample restrictions
- author_fields: extract research fields of authors
    - Three FieldClasses
        - "first", field at level 1 over publications in first `years_first_field`
        - "last", field at level 1 over publicaitons in last `years_last_field`
        - "main", field at level 0 over all publications

"""

import sqlite3 as sqlite
import warnings
import time 
import argparse
from helpers.functions import print_elapsed_time, analyze_db
from helpers.variables import db_file, insert_questionmark_doctypes, keep_doctypes


# ## Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--years_first_field", type = int, default = 5,
                    help="How many years to consider when calculating the first field of the author?")
parser.add_argument("--years_last_field", type = int, default = 5,
                    help="How many years to consider when calculating the last field of the author?")
args = parser.parse_args()

# ## Variables; connect to db
start_time = time.time()
print(f"Start time: {start_time} \n")

con = sqlite.connect(database = db_file, isolation_level= None)


# ## PaperAuthorUnique 
#   This is necessary because some authors publish papers with multiple affiliations 
#   Condition on papers being in PaperMainFieldOfStudy 
#   table is important for a lot of what follows

print("Making PaperAuthorUnique table ...\n")
con.execute("DROP TABLE IF EXISTS PaperAuthorUnique")
con.execute("""CREATE TABLE PaperAuthorUnique AS 
            SELECT DISTINCT PaperId, AuthorId 
            FROM PaperAuthorAffiliations 
            INNER JOIN (
                SELECT PaperId 
                FROM PaperMainFieldsOfStudy 
            ) USING (PaperId)
            """)
con.execute("CREATE UNIQUE INDEX idx_pau_PaperAuthorId ON PaperAuthorUnique (PaperId ASC, AuthorId ASC)")
con.execute("CREATE INDEX idx_pau_AuthorId ON PaperAuthorUnique (AuthorId)") 

# ## author_sample   
print_elapsed_time(start_time)

print(f"Author sample with career restrictions, keeping DocTypes {keep_doctypes}... \n")

con.execute("DROP TABLE IF EXISTS author_sample")
con.execute(f"""CREATE TABLE 
                    author_sample AS 
                SELECT * 
                FROM (
                    SELECT   
                        a.AuthorId, 
                        MAX(b.Year) as YearLastPub, 
                        MIN(b.Year) as YearFirstPub,
                        COUNT(b.PaperId) as PaperCount  -- ## no UNIQUE necessary b/c already filtered above
                    FROM 
                        PaperAuthorUnique a
                    INNER JOIN (
                        SELECT PaperId, DocType, Year 
                        FROM Papers 
                    ) b ON a.PaperId = b.PaperId 
                    WHERE 
                        b.DocType IN ({insert_questionmark_doctypes})
                    GROUP BY a.AuthorId 
                    HAVING  
                        PaperCount >= 2 
                    AND 
                        PaperCount / (YearLastPub - YearFirstPub) <= 20 
                    ) AS c
                INNER JOIN (
                    SELECT AuthorId, 
                            SUBSTR(TRIM(NormalizedName),1,instr(trim(NormalizedName)||' ',' ')-1) AS FirstName
                    FROM Authors 
                ) d USING (AuthorId)
                """, (keep_doctypes)
                )

con.execute("CREATE UNIQUE INDEX idx_as_AuthorId ON author_sample (AuthorId ASC) ")
con.execute("CREATE INDEX idx_as_FirstName ON author_sample (FirstName ASC)")

# ## author_fields 
    # for now, follow Huang et al: assign the most common field to each author.
    # use the first couple of years of publishing
        # "exogenous" to later career outcomes 
        # for matching the graduates later on, we might need a more fine-grained measure? 

# ### Idea: Take most likely field, but also keep some summary stats
    # to inform how well we measure the specific fields
    # temp1: all observed author-field combinations
    # temp2: add share of score per combination
    # author_fields: join temp2 and aggregate temp2 at author-FieldClass level

print_elapsed_time(start_time)
print(f"Making table author_fields:")
print(f"\t N years for first field: {args.years_first_field}")
print(f"\t N years for last field: {args.years_last_field}")
    
# ### All author-field combinations observed
    # Some papers have missing fieldofstudyid. In this case, prefer papers with known field of study id
        # In order to keep information on authors that only publish papers with missing field of study id,
        # assign to papers with missing field of study id a very small score. Dropping them would drop authors from the table
            # that only publish papers with missing field of study id.
con.execute("DROP TABLE IF EXISTS temp1")
con.execute(f""" CREATE TEMPORARY TABLE temp1 AS 
            SELECT * 
            FROM (
                -- ## First fields: field at level 1 in first years of career 
                SELECT  a.AuthorId, 
                        COUNT(a.PaperId) AS PaperCount, 
                        SUM(d.SourceFieldScore) AS TotalSourceFieldScore,
                        d.FieldOfStudyId,
                        "first" AS FieldClass 
                FROM PaperAuthorUnique a
                INNER JOIN ( -- ## TODO: check inner or left join necessary?. I think INNER should be fine
                    SELECT AuthorId, YearFirstPub 
                    FROM author_sample 
                ) b USING (AuthorId) 
                INNER JOIN ( 
                    SELECT PaperId, Year 
                    FROM Papers 
                    WHERE 
                        DocType IN ({insert_questionmark_doctypes}) 
                        AND 
                        DocType IS NOT NULL 
                ) c USING (PaperId) 
                INNER JOIN (
                    SELECT e.PaperId, e.Field1 AS FieldOfStudyId,
                        CASE WHEN e.Field1 IS NULL THEN 0.00000000001 ELSE f.Score END AS SourceFieldScore -- ## use small number to prevent problems with ScoreShare below
                    FROM PaperMainFieldsOfStudy e 
                    INNER JOIN PaperFieldsOfStudy f ON (e.PaperId = f.PaperId AND e.OriginalFieldId = f.FieldOfStudyId)
                ) d USING (PaperId) 
                WHERE c.Year <= b.YearFirstPub + (?) -- ## Variable input here, see bottom of the query
                GROUP BY a.AuthorId, d.FieldOfStudyId 
                UNION 
                -- ## Last fields: field at level 1 in last years of career 
                SELECT  a.AuthorId, 
                        COUNT(a.PaperId) AS PaperCount, 
                        SUM(d.SourceFieldScore) AS TotalSourceFieldScore,
                        d.FieldOfStudyId,
                        "last" AS FieldClass 
                FROM PaperAuthorUnique a
                INNER JOIN ( -- ## TODO: check inner or left join necessary?. I think INNER should be fine
                    SELECT AuthorId, YearLastPub 
                    FROM author_sample 
                ) b USING (AuthorId) 
                INNER JOIN ( 
                    SELECT PaperId, Year 
                    FROM Papers 
                    WHERE 
                        DocType IN ({insert_questionmark_doctypes}) 
                        AND 
                        DocType IS NOT NULL 
                ) c USING (PaperId) 
                INNER JOIN (
                    SELECT e.PaperId, e.Field1 AS FieldOfStudyId,
                        CASE WHEN e.Field1 IS NULL THEN 0.00000000001 ELSE f.Score END AS SourceFieldScore -- ## use small number to prevent problems with ScoreShare below
                    FROM PaperMainFieldsOfStudy e 
                    INNER JOIN PaperFieldsOfStudy f ON (e.PaperId = f.PaperId AND e.OriginalFieldId = f.FieldOfStudyId)
                ) d USING (PaperId) 
                WHERE c.Year >= b.YearLastPub - (?) -- ## Variable input here, see bottom of the query
                GROUP BY a.AuthorId, d.FieldOfStudyId
                UNION
                -- ## Main fields: fields at level 0 ever published by the author 
                SELECT  a.AuthorId,
                        COUNT(a.PaperId) AS PaperCount, 
                        SUM(d.SourceFieldScore) AS TotalSourceFieldScore,
                        d.FieldOfStudyId, 
                        "main" AS FieldClass 
                FROM PaperAuthorUnique a 
                INNER JOIN (
                    SELECT AuthorId 
                    FROM author_sample 
                ) b USING (AuthorId) 
                INNER JOIN ( 
                    SELECT PaperId, Year 
                    FROM Papers 
                    WHERE 
                        DocType IN ({insert_questionmark_doctypes}) 
                        AND 
                        DocType IS NOT NULL 
                ) c USING (PaperId) 
                INNER JOIN (
                    SELECT e.PaperId, e.Field0 AS FieldOfStudyId, 
                     CASE WHEN e.Field0 IS NULL THEN 0.00000000001 ELSE f.Score END AS SourceFieldScore -- ## use small number to prevent problems with ScoreShare below
                    FROM PaperMainFieldsOfStudy e 
                    INNER JOIN PaperFieldsOfStudy f ON (e.PaperId = f.PaperId AND e.OriginalFieldId = f.FieldOfStudyId)
                ) d USING (PaperId) 
                GROUP BY a.AuthorId, d.FieldOfStudyId 
            ) 
            """,
            (keep_doctypes + (args.years_first_field,) + keep_doctypes + (args.years_last_field,) + keep_doctypes)
            )

# ### Add share score by AuthorId-FieldClass
con.execute("DROP TABLE IF EXISTS temp2")
con.execute("""CREATE TEMPORARY TABLE temp2 AS 
            SELECT *
            FROM (
                SELECT  *,
                    ( TotalSourceFieldScore / (
                        SUM(TotalSourceFieldScore) OVER(
                        PARTITION BY AuthorId, FieldClass )  
                        )
                    ) AS ScoreShare -- ## NOTE: this could lead to problems when an author only publishes papers with 0 scores. But does not seem to occur in practice.
                FROM temp1
                INNER JOIN (
                    SELECT AuthorId
                    FROM author_sample
                ) USING (AuthorId)
            )
            """)
con.execute("CREATE UNIQUE INDEX idx_tt2_AuthorClass ON temp2 (AuthorId ASC, FieldClass ASC) ")

con.execute("DROP TABLE IF EXISTS author_fields")
con.execute("""CREATE TABLE author_fields AS 
            SELECT  a.AuthorId,
                    a.FieldOfStudyId,
                    a.FieldClass,
                    a.TotalSourceFieldScore as Score,
                    b.FieldOfStudyCount, 
                    b.HHIAllFields,
                    b.SumScoreAllFields,
                    b.PaperCountAllFields
            FROM temp2 a
            INNER JOIN (
                SELECT  AuthorId, 
                        FieldClass,
                        COUNT(FieldOfStudyId) AS FieldOfStudyCount,
                        SUM(PaperCount) AS PaperCountAllFields, 
                        SUM(TotalSourceFieldScore) AS SumScoreAllFields,
                        SUM(ScoreShare * ScoreShare) AS HHIAllFields,
                        MAX(ScoreShare) AS MaxScoreShare
                FROM temp2 
                GROUP BY AuthorId, FieldClass
            ) b USING (AuthorId, FieldClass)
            WHERE a.ScoreShare = b.MaxScoreShare
            """)

con.execute("CREATE UNIQUE INDEX idx_af_AuthorField ON author_fields (AuthorId ASC, FieldClass, FieldOfStudyId)")

# ## Run ANALYZE, finish
analyze_db(con)

con.close()

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")

