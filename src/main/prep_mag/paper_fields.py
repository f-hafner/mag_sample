#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script paper_fields.py

Generate tables
- crosswalk_fields: corresponds FieldOfStudyIds to Ids further up in the hierarchy. Specifically,
    - corresponds any FieldOfStudyId at Level > 0 to a FieldOfStudyId at Level = 0
    - corresponds any FieldOfStudyId at Level > 1 to a FieldOfStudyId at Level = 1
    - *Purpose*: correspond papers to their most likely FieldOfStudy at the two first levels, if for instance
        they only have an Id at level 3.
    - Principle of how the table is constructed: count papers and the sum of the score of papers falling in 
        field 0A and field 1B (number = level, letter = field id).
        Thus, it is possible that there is no correspondence from field 2C to field 0A if no
        paper is ever assigned both field 2C and 0A. 
    - I have not checked in detail if this makes a lot of sense. For instance, I have seen that 
        "Industrial Organization" is assigned to field0 "Business" instead of "Economics", but for some other 
        field the crosswalk seemed more intuitive.
- PaperMainFieldsOfStudy: 
    - based on crosswalk_fields, assigns a unique FieldOfStudyId from Level 0 and Level 1 to each paper
    - *Purpose*: for studying publication patterns in specific fields (level 0), and possible for 
        field-specific Fixed Effects (level 1) in analysis. 
    - *Note*: some papers do not have such a field assigned because they are only assigned a field
        in some level > 1 for which there is no correspondence to levels 0 or 1 (see above).
"""

# ## TODO
    # PaperMainFieldsOfStudy: Report paper count by Level and SourcFieldLevel ?
    # condition the crosswalk on "useful" doctypes? also condition the PaperMainFieldOfStudy table on the same restriction? what is a sensible solution here?
    # create new random category with all papers that have a missing fieldofstudy? there also >1Mio books.

import sqlite3 as sqlite
import time 
import pandas as pd

from helpers.functions import print_elapsed_time, analyze_db
from helpers.variables import db_file, insert_questionmark_doctypes, keep_doctypes

# ## Variables; connect to db
start_time = time.time()
print(f"Start time: {start_time} \n")

con = sqlite.connect(database = db_file, isolation_level= None)


# ## Crosswalk from any field with level > 0 to field with level 0 or 1
print("Making crosswalk_fields ... \n")
con.execute("DROP TABLE IF EXISTS paper_fields_level")
con.execute("""CREATE TEMPORARY TABLE paper_fields_level AS 
            SELECT  a.PaperId, 
                    a.FieldOfStudyId, 
                    a.Score, 
                    b.Level,
                    MIN(b.Level) OVER(PARTITION BY PaperId) AS MinLevel,
                    Count(a.FieldOfStudyId) OVER(PARTITION BY PaperId) AS FieldOfStudyCount 
            FROM PaperFieldsOfStudy a
            INNER JOIN FieldsOfStudy b USING(FieldOfStudyId)
            """)
con.execute("CREATE INDEX idx_tpfl_PaperId on paper_fields_level (PaperId)")

con.execute("DROP TABLE IF EXISTS crosswalk_fields")
con.execute("""CREATE TABLE crosswalk_fields AS 
            SELECT * 
            FROM (
                -- ## Correspond any field with level > 0 to field with level 0 ##
                SELECT * 
                FROM (
                    SELECT  b.FieldOfStudyId AS ParentFieldOfStudyId,
                            a.FieldOfStudyId AS ChildFieldOfStudyId,
                            COUNT(DISTINCT a.PaperId) AS PaperCount, 
                            SUM(a.Score) AS TotalScore, 
                            b.Level AS ParentLevel, 
                            a.Level AS ChildLevel
                    FROM paper_fields_level a
                    INNER JOIN paper_fields_level b USING(PaperId)
                    WHERE (
                        a.Level > 0 AND
                        b.Level = 0
                    ) 
                    GROUP BY ParentFieldOfStudyId, ChildFieldOfStudyId 
                )
                GROUP BY ChildFieldOfStudyId 
                HAVING TotalScore = MAX(TotalScore) 
                UNION 
                -- ## Correspond any field with level > 1 to field with level 1 ## 
                SELECT * 
                FROM (
                    SELECT  b.FieldOfStudyId AS ParentFieldOfStudyId,
                            a.FieldOfStudyId AS ChildFieldOfStudyId,
                            COUNT(DISTINCT a.PaperId) AS PaperCount, 
                            SUM(a.Score) AS TotalScore, 
                            b.Level AS ParentLevel, 
                            a.Level AS ChildLevel
                    FROM paper_fields_level a
                    INNER JOIN paper_fields_level b USING(PaperId)
                    WHERE (
                        a.Level > 1 AND
                        b.Level = 1
                    ) 
                    GROUP BY ParentFieldOfStudyId, ChildFieldOfStudyId 
                )
                GROUP BY ChildFieldOfStudyId 
                HAVING TotalScore = MAX(TotalScore) 
            )
            """)
con.execute("CREATE UNIQUE INDEX idx_cf_ParentChildField ON crosswalk_fields (ParentFieldOfStudyId, ChildFieldOfStudyId, ParentLevel, ChildLevel) ")

print_elapsed_time(start_time)

# ## Assign papers to main fields 
print("Making PaperMainFieldsOfStudy... \n")

con.execute("DROP TABLE IF EXISTS paper_field_maxscore")
con.execute("""CREATE TEMPORARY TABLE paper_field_maxscore AS 
            SELECT a.PaperId, a.FieldOfStudyId, a.Score, b.Level
            FROM PaperFieldsOfStudy a
            INNER JOIN FieldsOfStudy b USING (FieldOfStudyId)
            WHERE b.Level > 0
            GROUP BY PaperId
            HAVING Score = Max(Score)
            """)
con.execute("CREATE UNIQUE INDEX idx_pfm_PaperId on paper_field_maxscore (PaperId)")

con.execute("DROP TABLE IF EXISTS PaperMainFieldsOfStudy")
con.execute("""CREATE TABLE PaperMainFieldsOfStudy AS 
            SELECT c.PaperId, d.Field0, c.Field1, c.OriginalFieldId
            FROM (
                -- ## papers where the max score is at level > 1
                SELECT * 
                FROM (
                    SELECT a.PaperId, 
                    b.Field1,
                    a.FieldOfStudyId AS OriginalFieldId
                    FROM paper_field_maxscore a
                    INNER JOIN (
                        SELECT ParentFieldOfStudyId AS Field1, ChildFieldOfStudyId
                        FROM crosswalk_fields 
                        WHERE ChildLevel > 1 AND ParentLevel = 1
                    ) b ON (a.FieldOfStudyId = b.ChildFieldOfStudyId)
                    WHERE Level > 1
                )
                UNION ALL
                -- ## papers where the max score is at level 1
                SELECT * 
                FROM (
                    SELECT PaperId, 
                        FieldOfStudyId AS Field1,
                        FieldOfStudyId AS OriginalFieldId
                    FROM paper_field_maxscore 
                    WHERE Level = 1
                )
            ) c            
            LEFT JOIN (
                SELECT ParentFieldOfStudyId as Field0,
                        ChildFieldOfStudyId
                FROM crosswalk_fields 
                WHERE ParentLevel = 0
            ) d ON (c.Field1 = d.ChildFieldOfStudyId)
            UNION ALL 
            -- ## add the papers that have a Field0, but no field at a "lower" level 
            SELECT PaperId, 
                    FieldOfStudyId as Field0, 
                    NULL as Field1, 
                    FieldOfStudyId AS OriginalFieldId 
            FROM (
                SELECT c.PaperId, c.DocType, e.NormalizedName, e.Level, d.Score, d.FieldOfStudyId
                FROM Papers c
                INNER JOIN PaperFieldsOfStudy d USING(PaperId)
                INNER JOIN FieldsOfStudy e USING(FieldOfStudyId)
                WHERE c.PaperId NOT IN (SELECT PaperId FROM paper_field_maxscore)
                    AND e.Level = 0
                GROUP BY c.PaperId
                HAVING d.Score = Max(d.Score)
            )
""")

con.execute("CREATE UNIQUE INDEX idx_pmf_PaperId ON PaperMainFieldsOfStudy (PaperId ASC) ")
con.execute("CREATE INDEX idx_pmf_Field0 ON PaperMainFieldsOfStudy (Field0 ASC) ") # this is for quick sampling of papers by field of study id
con.execute("CREATE INDEX idx_pmf_Field1 ON PaperMainFieldsOfStudy (Field1 ASC) ") # this is for quick sampling of papers by field of study id

print_elapsed_time(start_time)

# ## Check coverage of papers by field0 and field1
summary_query = f"""
SELECT a.DocType,
    CASE 
        WHEN b.Field0 IS NOT NULL AND b.Field1 IS NOT NULL THEN "both_fields"
        WHEN b.Field1 IS NULL AND b.Field0 IS NOT NULL THEN "only_field0"
        WHEN b.Field0 IS NULL AND b.Field1 IS NOT NULL THEN "only_field1"
        WHEN b.Field0 IS NULL AND b.Field1 IS NULL THEN "no_field"
    END AS field_available,
    COUNT(a.PaperId) AS PaperCount, 
    SUM(a.CitationCount) AS CitationCount
FROM Papers a
LEFT JOIN (
    SELECT PaperId, Field0, Field1
    FROM PaperMainFieldsOfStudy
) b ON (a.PaperId = b.PaperId)
WHERE a.Year >= 1950 
    AND a.DocType IN ({insert_questionmark_doctypes})
    AND a.DocType IS NOT NULL 
GROUP BY a.DocType, field_available
"""

summary_coverage = pd.read_sql(con = con, sql = summary_query, params = keep_doctypes)
print(f"Among all papers in {keep_doctypes} since 1950, we have the following coverage of papers and total citations in PaperMainFieldsOfStudy:\n {summary_coverage} \n")

# ## Run ANALYZE, finish 
analyze_db(con)

con.close()

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")

