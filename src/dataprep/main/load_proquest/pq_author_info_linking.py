
"""
Collapse some of the information from proquest into a single string
For usage in linking scripts. 

Create 1 table in database:
- pq_info_linking
    - collapsed fields to be used as keywords in graduate linking
    - collapsed advisors to be used as co-authors in graduate linking
"""

import sqlite3 as sqlite
import time 

from helpers.variables import db_file
from helpers.functions import analyze_db, print_elapsed_time

start_time = time.time()

con = sqlite.connect(database = db_file, isolation_level= None)

interactive = True 
query_limit = ""
if interactive:
    query_limit = "LIMIT 10"


with con:
    print("Collapsing fields...", flush=True)
    con.execute(f"""
    CREATE TEMP TABLE pq_fields_collapsed AS 
    SELECT goid
            , GROUP_CONCAT(fieldname, ";") AS fields_lvl1
    FROM (
        SELECT * 
        FROM (
              -- ## (1) fields at level 2 and higher 
            SELECT DISTINCT goid, parent_name AS fieldname
            from (select * from pq_magfos where score >0.4 limit 5000000000) as pq_magfos
            INNER JOIN (
                SELECT goid 
                FROM pq_authors
            ) USING(goid)
            -- ## Aggregate to the parent field at level 1
            INNER JOIN (
                SELECT ChildFieldOfStudyId, parent_name 
                FROM crosswalk_fields AS a 
                INNER JOIN (
                    SELECT FieldOfStudyId, NormalizedName AS parent_name 
                    FROM FieldsOfStudy
                ) b
                ON (a.ParentFieldOfStudyId = b.FieldOfStudyId)
                WHERE ParentLevel = 1
            ) 
            ON (pq_magfos.FieldOfStudyId = ChildFieldOfStudyId)
            UNION 
            -- ## (2) fields at level 1
            SELECT DISTINCT goid, fieldname 
            from (select * from pq_magfos where score >0.4 limit 5000000000) as pq_magfos
            INNER JOIN ( 
                SELECT goid 
                FROM pq_authors
            ) USING(goid)
            INNER JOIN (
                SELECT FieldOfStudyId, NormalizedName AS fieldname
                FROM FieldsOfStudy 
                WHERE Level = 1 
            ) USING(FieldOfStudyId)
        )
        ORDER BY fieldname 
    ) 
    GROUP BY goid 
    """)

    con.execute("CREATE INDEX idx_pfc_goid on pq_fields_collapsed (goid ASC)")

    print_elapsed_time(start_time)
    print("Collapsing advisors...", flush=True)
    con.execute(f"""
    CREATE TEMP TABLE pq_advisors_collapsed AS 
    SELECT goid, GROUP_CONCAT(fullname, ";") AS advisors
    FROM (
        SELECT goid, firstname || " " || lastname as fullname 
        FROM pq_advisors
    )
    INNER JOIN (
        SELECT goid 
        FROM pq_authors 
        {query_limit}
    ) USING(goid)
    GROUP BY goid
    """)

    con.execute("CREATE UNIQUE INDEX idx_pac_goid ON pq_advisors_collapsed (goid ASC)")

    print_elapsed_time(start_time)
    print("Making final pq_info_linking table")
    con.execute("DROP TABLE IF EXISTS pq_info_linking")
    con.execute("""
    CREATE TABLE pq_info_linking AS 
    SELECT a.goid, b.fields_lvl1, c.advisors
    FROM pq_authors AS a
    LEFT JOIN (
        SELECT *
        FROM pq_fields_collapsed
    ) AS b
    USING(goid)
    LEFT JOIN (
        SELECT *
        FROM pq_advisors_collapsed
    ) AS c
    USING(goid)
    """)

    con.execute("CREATE UNIQUE INDEX idx_pil_goid ON pq_info_linking (goid ASC)")

    analyze_db(con)


time_to_run = (time.time() - start_time)/60
print(f"Done in {time_to_run} minutes.")



