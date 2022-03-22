#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script author_gender.py
Consolidate the gender information per author in author_sample
    Take Probability from FirstNamesGender
    Add most likely gender from other names when unclear 
    Flag the source of the gender variable
Output
- table author_gender with 
    - AuthorId 
    - NameUsed: the name that was used to infer gender 
    - NamePositionUsed: position in the full name of NameUsed. Filter NamePositionUsed = 0 to get only 
        authors where the first part of the name was used for gender
    - PersonCount: number of persons underlying the calculation on genderize
    - ProbabilityFemale
"""

# TODO
    # how many Asians do we recover like this? can we do better?



import sqlite3 as sqlite
import time 

from helpers.functions import print_elapsed_time, analyze_db
from helpers.variables import db_file

# ## Variables; connect to db
start_time = time.time()
print(f"Start time: {start_time} \n")

con = sqlite.connect(database = db_file, isolation_level= None)


# ## Temp table for new gender where probability is low 
print("Making temp table new_gender \n")
con.execute("DROP TABLE IF EXISTS new_gender")
con.execute("""CREATE TEMP TABLE new_gender AS 
                SELECT a.AuthorId
                    , a.Name AS FirstName
                    , b.OldProbability
                    , b.OldGender
                    , b.OldPersonCount
                    , e.NewFirstNameForGender
                    , e.PositionNewFirstName
                    , e.NewProbability
                    , e.NewGender
                    , e.NewPersonCount
                FROM AuthorNameSplits a 
                INNER JOIN (
                    SELECT FirstName, 
                        CASE 
                        WHEN ProbabilityFemale > 0.5 THEN ProbabilityFemale
                        WHEN ProbabilityFemale < 0.5 THEN 1 - ProbabilityFemale 
                        END AS OldProbability,
                        CASE 
                        WHEN ProbabilityFemale > 0.5 THEN "Female"
                        WHEN ProbabilityFemale < 0.5 THEN "Male"
                        END AS OldGender,
                        PersonCount AS OldPersonCount
                    FROM FirstNamesGender
                ) b ON (a.Name = b.FirstName)
                INNER JOIN (
                    SELECT c.AuthorId
                        , c.Name AS NewFirstNameForGender
                        , c.Position AS PositionNewFirstName 
                        , d.NewProbability
                        , d.NewGender
                        , d.NewPersonCount
                    FROM AuthorNameSplits c 
                    INNER JOIN (
                        SELECT FirstName, 
                        CASE 
                        WHEN ProbabilityFemale > 0.5 THEN ProbabilityFemale
                        WHEN ProbabilityFemale < 0.5 THEN 1 - ProbabilityFemale 
                        END AS NewProbability,
                        CASE 
                        WHEN ProbabilityFemale > 0.5 THEN "Female"
                        WHEN ProbabilityFemale < 0.5 THEN "Male"
                        END AS NewGender,
                        PersonCount AS NewPersonCount 
                        FROM UnclearNamesGender
                    ) d ON (c.Name = d.FirstName)
                    WHERE c.Position > 0
                    GROUP BY c.AuthorId 
                    HAVING d.NewProbability = MAX(d.NewProbability)
                ) e USING (AuthorId)
                -- keep only authors in author_sample
                INNER JOIN (SELECT AuthorId FROM author_sample) USING (AuthorId)
                WHERE a.Position = 0
""")

print_elapsed_time(start_time)

# ## assign gender probability to all authors in author_sample 
print("Making table author_gender \n")
con.execute("DROP TABLE IF EXISTS author_gender")
con.execute("""CREATE TABLE author_gender AS 
            -- Authors with high probability 
            SELECT a.AuthorId
                    , a.FirstName AS NameUsed
                    , 0 as NamePositionUsed
                    , b.PersonCount 
                    , b.ProbabilityFemale
            FROM author_sample a 
            INNER JOIN (
                SELECT * 
                FROM FirstNamesGender
                WHERE ProbabilityFemale >= 0.8 OR ProbabilityFemale <= 0.2
            ) b USING (FirstName)
            UNION ALL 
            -- Authors with low probability but high probability for other part of the name 
            SELECT AuthorId
                    , NewFirstNameForGender AS NameUsed
                    , PositionNewFirstName AS NamePositionUsed
                    , NewPersonCount AS PersonCount 
                    , CASE 
                        WHEN NewGender = "Male" THEN 1 - NewProbability
                        WHEN NewGender = "Female" THEN NewProbability 
                    END AS ProbabilityFemale
            FROM new_gender 
            WHERE NewProbability >= 0.8 -- OldProbability is by default < 0.8
                AND NewPersonCount > 5
                AND OldGender = NewGender
""")

con.execute("CREATE UNIQUE INDEX idx_ag_AuthorId ON author_gender (AuthorId ASC)")
con.execute("CREATE INDEX idx_ag_NameUsed ON author_gender (NameUsed)")

print_elapsed_time(start_time)

# ## Run ANALYZE, finish
analyze_db(con)

con.close()

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")

