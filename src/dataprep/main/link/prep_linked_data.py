#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script prep_linked_data.py

Generate linking tables:
- current_links
- current_links_advisors

For AuthorIds in either of them generate
- author_panel
- author_citations
- author_output

*Note*
- An author only appears in one field, but citations can come from papers in any field
- AKM model is estimated on papers in one field; thus, an author may have estimated FEs for different fields
- The output depends on whether links to PQ are used or not.

"""

# ## TODO
# when using links, it seems that some of the linked ids do not end up in author panel. Why?

import sqlite3 as sqlite
import time 
import pandas as pd
from helpers.functions import print_elapsed_time, analyze_db
from helpers.variables import db_file, insert_questionmarks_doctypes, keep_doctypes

# ## Arguments
# parser = argparse.ArgumentParser()
# parser.set_defaults(use_links = True)
# args = parser.parse_args()


# ## Variables; connect to db
max_yeardiff = 5
start_time = time.time()
print(f"Start time: {start_time} \n", flush = True)
print(f"Using the following DocTypes for citations: {keep_doctypes}... \n")


con = sqlite.connect(database = db_file, isolation_level= None)

# ## (0) Delete all tables that are created in this script.
    # Necessary b/c not all fields have links, and when links are used, the script does not run through
print("Deleting existing tables")
tables_to_delete = ["author_citations", "author_output", "author_panel", 
                    "current_links", "current_links_advisors",
                    "links_currentfield"] # links_currentfield is from extract_field.py
for tbl in tables_to_delete:
    con.execute(f"""DROP TABLE IF EXISTS {tbl}""")


# ## (1) Get linked ids for given specs.
    # NOTE: count(distinct authorid) not possible and not necessary: because each authorid is uniquely in one field0, any goid that has multiple links
        # will be linked to different authorids because they come from different fields.
print("current_links for graduates", flush = True)
con.execute("""
CREATE TEMP TABLE current_links_temp AS 
SELECT AuthorId, goid, link_score, iteration_id 
FROM (
    SELECT a.*, COUNT(AuthorId) OVER(PARTITION BY goid) AS n_links  
    FROM linked_ids a
    INNER JOIN (
        SELECT iteration_id 
        FROM (
            SELECT iteration_id, MAX(iteration_id) AS max_id 
            FROM linking_info
            WHERE  mergemode = '1:1'
                AND fieldofstudy_str = 'False'
                AND fieldofstudy_cat = 'False'
                AND institution = 'False'
                AND keywords = 'False'
                AND testing = 0 
                AND recall = 0.9
            GROUP BY field
        )
        WHERE iteration_id = max_id 
    ) b USING(iteration_id)
    WHERE link_score > 0.7
)
WHERE n_links = 1
""")

con.execute("CREATE UNIQUE INDEX idx_lcf_AuthorIdgoid ON current_links_temp (AuthorId ASC, goid ASC)")
con.execute("CREATE UNIQUE INDEX idx_lcf_goid ON current_links_temp (goid ASC)") # this is also a way to make sure there are not multiple links per goid

# Drop links where year first and last pub are too far apart 
    # make temp table above, index as now; repeat
con.execute("""
CREATE TABLE current_links AS 
SELECT a.* 
FROM current_links_temp a
INNER JOIN (
    SELECT AuthorId, YearFirstPub
    FROM author_sample
) b USING(AuthorId)
INNER JOIN (
    SELECT goid, degree_year
    FROM pq_authors
) c USING(goid)
WHERE b.YearFirstPub <= c.degree_year + (?)
    AND b.YearFirstPub >= c.degree_year - (?)
""",
(max_yeardiff, max_yeardiff, )
)

con.execute("CREATE UNIQUE INDEX idx_t_AuthorIdgoid ON current_links (AuthorId ASC, goid ASC)")
con.execute("CREATE UNIQUE INDEX idx_t_goid ON current_links (goid ASC)") # this is also a way to make sure there are not multiple links per goid

# ## (1) Get linked advisors for given specs.
print("current_links for advisors", flush = True)
con.execute("""
CREATE TABLE current_links_advisors AS 
SELECT AuthorId, relationship_id, link_score, iteration_id 
FROM (
    SELECT a.*, COUNT(AuthorId) OVER(PARTITION BY relationship_id) AS n_links  
    FROM linked_ids_advisors a
    INNER JOIN (
        SELECT iteration_id 
        FROM (
            SELECT iteration_id, MAX(iteration_id) AS max_id 
            FROM linking_info_advisors
            WHERE  mergemode = 'm:1'
                AND fieldofstudy_str = 'False'
                AND fieldofstudy_cat = 'False'
                AND institution = 'True'
                AND keywords = 'False'
                AND testing = 0 
                AND recall = 0.9
            GROUP BY field
        )
        WHERE iteration_id = max_id 
    ) b USING(iteration_id)
    WHERE link_score > 0.7
)
WHERE n_links = 1
""")

# for now, do not condition on certain time distance between 
# graduation year and whenever the supervisor has a publication. 
# TODO: do this after gaining some insights in the analysis

con.execute("CREATE UNIQUE INDEX idx_cla_AuthorIdrelid ON current_links_advisors (AuthorId ASC, relationship_id ASC)")
con.execute("CREATE UNIQUE INDEX idx_cla_relid ON current_links_advisors (relationship_id ASC)") # this is also a way to make sure there are not multiple links per goid

# ## (2) Helper table for later operations
con.execute("DROP TABLE IF EXISTS current_authors")
con.execute("""
CREATE TEMPORARY TABLE current_authors AS 
SELECT AuthorId 
FROM author_sample 
INNER JOIN (
    SELECT AuthorId 
    FROM current_links
    UNION 
    SELECT AuthorId
    FROM current_links_advisors
) USING (AuthorId)
""")

con.execute("CREATE UNIQUE INDEX idx_tacf_AuthorId ON current_authors (AuthorId)")


# ## (3) Author panel 
con.execute("""
CREATE TABLE author_panel AS 
SELECT  a.AuthorId, b.Year
FROM author_sample a 
INNER JOIN (
    SELECT AuthorId 
    FROM current_authors
) USING (AuthorId) 
INNER JOIN (
    SELECT AuthorId, goid
    FROM current_links
) c USING (AuthorId)
INNER JOIN (
    SELECT goid, degree_year
    FROM pq_authors
) d USING (goid)
CROSS JOIN (
    SELECT DISTINCT Year 
    FROM Papers
) b 
WHERE 
    b.Year >= d.degree_year - 5
    AND 
    b.Year <= a.YearLastPub 
""")

con.execute("CREATE UNIQUE INDEX idx_ap_AuthorIdYear on author_panel (AuthorId ASC, Year)")
# read_con.execute('CREATE INDEX idx_ap_AuthorIdYearsExperience on author_panel (AuthorId ASC, YearsExperience ASC)')

print_elapsed_time(start_time)


# ## (4) author_citations
print("Making author_citations... \n")

con.execute(f"""
CREATE TABLE author_citations AS 
SELECT  a.AuthorId, 
        b.Year, 
        SUM(b.CitationCount) AS CitationCount -- ## this counts citations of papers in each year they made, up to 10 years after publication
FROM PaperAuthorUnique a 
INNER JOIN paper_citations b 
    ON a.PaperId = b.PaperReferenceId 
INNER JOIN (
    SELECT PaperId, Year AS PublicationYear
    FROM Papers
    WHERE 
        DocType IN ({insert_questionmarks_doctypes}) 
        AND 
        DocType IS NOT NULL 
) c USING(PaperId)
INNER JOIN (
    SELECT AuthorId
    FROM current_authors
) USING(AuthorId)
WHERE b.ReferencingDocType IN ({insert_questionmarks_doctypes}) 
    AND b.Year <= 10 + c.PublicationYear 
GROUP BY a.AuthorId, b.Year
""",
(keep_doctypes + keep_doctypes)
)
con.execute("CREATE UNIQUE INDEX idx_ac_AuthorIdYear on author_citations (AuthorId ASC, Year)")
#con.execute("CREATE INDEX idx_ac_ReferencingDocType on author_citations (ReferencingDocType)")

print_elapsed_time(start_time)



# ## (5) author_output
print("Making author_output... \n")

con.execute(f"""CREATE TABLE author_output AS 
                SELECT  a.AuthorId,
                        d.Year,
                        COUNT(a.PaperId) AS PaperCount,  -- ## DISTINCT not necessary when summarising a at the author level
                        SUM(b.AuthorCount) AS TotalAuthorCount,
                        SUM(b.CitationCount_y10) AS TotalForwardCitations -- ## this measures the impact of the paper at publication
                FROM PaperAuthorUnique a
                INNER JOIN (
                    SELECT AuthorId
                    FROM current_authors
                ) USING(AuthorId)
                INNER JOIN (
                    SELECT PaperId, Year
                    FROM Papers 
                    WHERE 
                        DocType IN ({insert_questionmarks_doctypes}) 
                        AND 
                        DocType IS NOT NULL 
                ) d USING (PaperId)
                INNER JOIN paper_outcomes b USING(PaperId) 
                GROUP BY a.AuthorId, d.Year
            """,
            keep_doctypes
            )

con.execute("CREATE UNIQUE INDEX idx_ao_AuthorIdYear on author_output (AuthorId ASC, Year)")
#con.execute("CREATE INDEX idx_ao_DocType on author_output (DocType) ")

print_elapsed_time(start_time)


# ## (6) Run ANALYZE, finish 
analyze_db(con)
con.close()

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")
