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
from helpers.variables import db_file, insert_questionmark_doctypes, keep_doctypes
import pdb 
import argparse

# ## Arguments
parser = argparse.ArgumentParser(description = 'Inputs for author_collab')
parser.add_argument("--filter_trainname_graduates", 
                    type=str,
                    dest = "filter_trainname_graduates", 
                    default=None,
                    help = "Filter the linking iterations of graduates by train name. If not given, use default settings defined in script.") 
parser.add_argument("--filter_trainname_advisors", 
                    type=str,
                    dest = "filter_trainname_advisors", 
                    default=None,
                    help = "Filter the linking iterations of advisors by train name. If not given, use default settings defined in script.") 
args = parser.parse_args()

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
    with con:
        con.execute(f"""DROP TABLE IF EXISTS {tbl}""")


if args.filter_trainname_graduates is not None:
    where_stmt_iterations_graduates = f"""
    WHERE train_name like '%{args.filter_trainname_graduates}%'
    """
if args.filter_trainname_advisors is not None:
    where_stmt_iterations_advisors = f"""
    WHERE train_name like '%{args.filter_trainname_advisors}%'
    """


print(f"{where_stmt_iterations_graduates=}", flush=True)
print(f"{where_stmt_iterations_advisors=}", flush=True)


# ## (1) Get linked ids for given specs.
    # NOTE: count(distinct authorid) not possible and not necessary: because each authorid is uniquely in one field0, any goid that has multiple links
        # will be linked to different authorids because they come from different fields.
print("current_links for graduates", flush = True)
con.execute(f"""
    CREATE TEMP TABLE current_links_temp AS 
    SELECT AuthorId, goid, link_score, iteration_id, rnk
    FROM (
        SELECT a.*
        , COUNT(AuthorId) OVER(PARTITION BY goid) AS n_links -- >1 if  same goid is linked to different MAG ids
        , COUNT(goid) OVER(PARTITION BY AuthorId) as n_links_authors  -- >1 if  same MAG id is linked to different goids
        , COUNT(*) OVER(PARTITION BY AuthorId, goid) AS n_same_links -- >1 if  same goid is linked to the same authorid in MAG in different fields
        , ROW_NUMBER() OVER ( 
            PARTITION BY AuthorId, goid
            ORDER BY link_score DESC
        ) AS rnk
        FROM linked_ids a
        INNER JOIN (
            SELECT iteration_id 
            FROM (
                SELECT iteration_id, MAX(iteration_id) AS max_id 
                FROM linking_info
                {where_stmt_iterations_graduates}
                GROUP BY field
            )
            WHERE iteration_id = max_id 
        ) b USING(iteration_id)
        WHERE link_score > 0.7
    )
    WHERE (
            (n_links = 1 and n_links_authors = 1) OR
            (n_links = n_same_links and n_links = n_links_authors)
        )
        AND rnk = 1
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
con.execute(f"""
CREATE TABLE current_links_advisors AS 
SELECT AuthorId, relationship_id, link_score, iteration_id 
FROM (
    SELECT a.*
        , COUNT(AuthorId) OVER(PARTITION BY relationship_id) AS n_links  
        , COUNT(*) OVER(PARTITION BY AuthorId, relationship_id) as n_same_links
        , ROW_NUMBER() OVER ( 
            PARTITION BY AuthorId, relationship_id
            ORDER BY link_score DESC
        ) AS rnk
    FROM linked_ids_advisors a
    INNER JOIN (
        SELECT iteration_id 
        FROM (
            SELECT iteration_id, MAX(iteration_id) AS max_id 
            FROM linking_info_advisors
            {where_stmt_iterations_advisors}
            GROUP BY field
        )
        WHERE iteration_id = max_id 
    ) b USING(iteration_id)
    WHERE link_score > 0.7
)
WHERE (n_links = 1 OR n_links = n_same_links)
    AND rnk = 1
""")

# for now, do not condition on certain time distance between 
# graduation year and whenever the supervisor has a publication. 
# Do this on the fly after gaining some insights in the analysis 

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
CROSS JOIN (
    SELECT DISTINCT Year 
    FROM Papers
) b 
WHERE 
    b.Year >= a.YearFirstPub - 20
    AND 
    b.Year <= a.YearLastPub 
""")

con.execute("CREATE UNIQUE INDEX idx_ap_AuthorIdYear on author_panel (AuthorId ASC, Year)")
# read_con.execute('CREATE INDEX idx_ap_AuthorIdYearsExperience on author_panel (AuthorId ASC, YearsExperience ASC)')

print_elapsed_time(start_time)


# ## (4) author_citations
print("Making author_citations... \n", flush = True)

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
        DocType IN ({insert_questionmark_doctypes}) 
        AND 
        DocType IS NOT NULL 
) c USING(PaperId)
INNER JOIN (
    SELECT AuthorId
    FROM current_authors
) USING(AuthorId)
WHERE b.ReferencingDocType IN ({insert_questionmark_doctypes}) 
    AND b.Year <= 10 + c.PublicationYear 
GROUP BY a.AuthorId, b.Year
""",
(keep_doctypes + keep_doctypes)
)
con.execute("CREATE UNIQUE INDEX idx_ac_AuthorIdYear on author_citations (AuthorId ASC, Year)")
#con.execute("CREATE INDEX idx_ac_ReferencingDocType on author_citations (ReferencingDocType)")

print_elapsed_time(start_time)



# ## (5) author_output
print("Making author_output... \n", flush = True)

con.execute("""
    CREATE TEMP TABLE main_family_papers AS
    SELECT PaperId
    FROM Papers
    WHERE PaperId = FamilyId OR FamilyId IS NULL OR FamilyId=''
""")
con.execute("CREATE UNIQUE INDEX idx_mfp_PaperId ON main_family_papers (PaperId)")

con.execute(f"""
    CREATE TEMP TABLE author_output_total AS 
    SELECT  a.AuthorId,
            d.Year,
            COUNT(a.PaperId) AS PaperCount,  -- ## DISTINCT not necessary when summarising a at the author level
            SUM(b.AuthorCount) AS TotalAuthorCount,
            SUM(b.CitationCount_y10) AS TotalForwardCitations, -- ## this measures the impact of the paper at publication
            SUM(c.new_word) AS new_word,
            SUM(c.new_word_reuse) AS new_word_reuse,
            SUM(c.new_bigram) AS new_bigram,
            SUM(c.new_bigram_reuse) AS new_bigram_reuse,
            SUM(c.new_trigram) AS new_trigram,
            SUM(c.new_trigram_reuse) AS new_trigram_reuse,
            SUM(c.new_word_comb) AS new_word_comb,
            SUM(c.new_word_comb_reuse) AS new_word_comb_reuse,
            MAX(c.cosine_max) AS cosine_max,
            AVG(c.cosine_max) AS avg_cosine_max,
            AVG(c.cosine_avg) AS avg_cosine_avg,
            SUM(c.n_words) AS n_words,
            SUM(c.n_bigrams) AS n_bigrams,
            SUM(c.n_trigrams) AS n_trigrams    
    FROM PaperAuthorUnique a
    INNER JOIN (
        SELECT AuthorId
        FROM current_authors
    ) USING(AuthorId)
    INNER JOIN (
        SELECT PaperId, Year
        FROM Papers 
        WHERE 
            DocType IN ({insert_questionmark_doctypes}) 
            AND 
            DocType IS NOT NULL 
    ) d USING (PaperId)
    INNER JOIN paper_outcomes b USING(PaperId) 
    LEFT JOIN novelty_reuse c USING(PaperId)
    GROUP BY a.AuthorId, d.Year
""",
keep_doctypes
)

con.execute(f"""
    CREATE TEMP TABLE author_output_mainpaper AS 
    SELECT  a.AuthorId,
            d.Year,
            COUNT(a.PaperId) AS PaperCount,
            SUM(b.AuthorCount) AS TotalAuthorCount,
            SUM(b.CitationCount_y10) AS TotalForwardCitations,
            SUM(c.new_word) AS new_word,
            SUM(c.new_word_reuse) AS new_word_reuse,
            SUM(c.new_bigram) AS new_bigram,
            SUM(c.new_bigram_reuse) AS new_bigram_reuse,
            SUM(c.new_trigram) AS new_trigram,
            SUM(c.new_trigram_reuse) AS new_trigram_reuse,
            SUM(c.new_word_comb) AS new_word_comb,
            SUM(c.new_word_comb_reuse) AS new_word_comb_reuse,
            MAX(c.cosine_max) AS cosine_max,
            AVG(c.cosine_max) AS avg_cosine_max,
            AVG(c.cosine_avg) AS avg_cosine_avg,
            SUM(c.n_words) AS n_words,
            SUM(c.n_bigrams) AS n_bigrams,
            SUM(c.n_trigrams) AS n_trigrams    
    FROM PaperAuthorUnique a
    INNER JOIN (
        SELECT AuthorId
        FROM current_authors
    ) USING(AuthorId)
    INNER JOIN (
        SELECT PaperId, Year
        FROM Papers 
        WHERE 
            DocType IN ({insert_questionmark_doctypes}) 
            AND 
            DocType IS NOT NULL 
    ) d USING (PaperId)
    INNER JOIN main_family_papers e USING(PaperId)
    INNER JOIN paper_outcomes b USING(PaperId) 
    LEFT JOIN novelty_reuse c USING(PaperId)
    GROUP BY a.AuthorId, d.Year
""",
keep_doctypes
)

con.execute("CREATE UNIQUE INDEX idx_aomp_AuthorIdYear on author_output_mainpaper (AuthorId ASC, Year)")

print("Making author_output for English papers... \n", flush = True)

con.execute(f"""
    CREATE TEMP TABLE author_output_total_english AS 
    SELECT  a.AuthorId,
            d.Year,
            COUNT(a.PaperId) AS PaperCount_english,
            SUM(b.AuthorCount) AS TotalAuthorCount_english,
            SUM(b.CitationCount_y10) AS TotalForwardCitations_english,
            SUM(c.new_word) AS new_word_english,
            SUM(c.new_word_reuse) AS new_word_reuse_english,
            SUM(c.new_bigram) AS new_bigram_english,
            SUM(c.new_bigram_reuse) AS new_bigram_reuse_english,
            SUM(c.new_trigram) AS new_trigram_english,
            SUM(c.new_trigram_reuse) AS new_trigram_reuse_english,
            SUM(c.new_word_comb) AS new_word_comb_english,
            SUM(c.new_word_comb_reuse) AS new_word_comb_reuse_english,
            MAX(c.cosine_max) AS cosine_max_english,
            AVG(c.cosine_max) AS avg_cosine_max_english,
            AVG(c.cosine_avg) AS avg_cosine_avg_english,
            SUM(c.n_words) AS n_words_english,
            SUM(c.n_bigrams) AS n_bigrams_english,
            SUM(c.n_trigrams) AS n_trigrams_english    
    FROM PaperAuthorUnique a
    INNER JOIN (
        SELECT AuthorId
        FROM current_authors
    ) USING(AuthorId)
    INNER JOIN (
        SELECT PaperId, Year
        FROM Papers 
        WHERE 
            DocType IN ({insert_questionmark_doctypes}) 
            AND 
            DocType IS NOT NULL 
    ) d USING (PaperId)
    INNER JOIN paper_language e ON a.PaperId = e.PaperId AND e.language = 'en'
    INNER JOIN paper_outcomes b USING(PaperId) 
    LEFT JOIN novelty_reuse c USING(PaperId)
    GROUP BY a.AuthorId, d.Year
""",
keep_doctypes
)
# join with novelty and reuse measure here. and add to author_output_total table and to author_first_author table

con.execute("CREATE UNIQUE INDEX idx_aot_AuthorIdYear on author_output_total (AuthorId ASC, Year)")
#con.execute("CREATE INDEX idx_ao_DocType on author_output (DocType) ")

con.execute(f"""
    CREATE TEMP TABLE author_output_firstauthor AS 
    SELECT  a.AuthorId,
            d.Year,
            COUNT(a.PaperId) AS PaperCount,  -- ## DISTINCT not necessary when summarising a at the author level
            SUM(b.AuthorCount) AS TotalAuthorCount,
            SUM(b.CitationCount_y10) AS TotalForwardCitations, -- ## this measures the impact of the paper at publication
            SUM(c.new_word) AS new_word,
            SUM(c.new_word_reuse) AS new_word_reuse,
            SUM(c.new_bigram) AS new_bigram,
            SUM(c.new_bigram_reuse) AS new_bigram_reuse,
            SUM(c.new_trigram) AS new_trigram,
            SUM(c.new_trigram_reuse) AS new_trigram_reuse,
            SUM(c.new_word_comb) AS new_word_comb,
            SUM(c.new_word_comb_reuse) AS new_word_comb_reuse,
            MAX(c.cosine_max) AS cosine_max,
            AVG(c.cosine_max) AS avg_cosine_max,
            AVG(c.cosine_avg) AS avg_cosine_avg,
            SUM(c.n_words) AS n_words,
            SUM(c.n_bigrams) AS n_bigrams,
            SUM(c.n_trigrams) AS n_trigrams 
    FROM PaperAuthorUnique a
    INNER JOIN (
        SELECT AuthorId
        FROM current_authors
    ) USING(AuthorId)
    INNER JOIN (
        SELECT AuthorId, PaperId
        FROM PaperAuthorAffiliations
        WHERE AuthorSequenceNumber = 1
    ) USING(AuthorId, PaperId)
    INNER JOIN (
        SELECT PaperId, Year
        FROM Papers 
        WHERE 
            DocType IN ({insert_questionmark_doctypes}) 
            AND 
            DocType IS NOT NULL 
    ) d USING (PaperId)
    INNER JOIN paper_outcomes b USING(PaperId) 
    LEFT JOIN novelty_reuse c USING(PaperId)
    GROUP BY a.AuthorId, d.Year
""",
keep_doctypes
)

con.execute("CREATE UNIQUE INDEX idx_aof_AuthorIdYear on author_output_firstauthor (AuthorId ASC, Year)")

con.execute(f"""
    CREATE TEMP TABLE author_output_lastauthor AS 
    SELECT  a.AuthorId,
            d.Year,
            COUNT(a.PaperId) AS PaperCount,
            SUM(b.AuthorCount) AS TotalAuthorCount,
            SUM(b.CitationCount_y10) AS TotalForwardCitations,
            SUM(c.new_word) AS new_word,
            SUM(c.new_word_reuse) AS new_word_reuse,
            SUM(c.new_bigram) AS new_bigram,
            SUM(c.new_bigram_reuse) AS new_bigram_reuse,
            SUM(c.new_trigram) AS new_trigram,
            SUM(c.new_trigram_reuse) AS new_trigram_reuse,
            SUM(c.new_word_comb) AS new_word_comb,
            SUM(c.new_word_comb_reuse) AS new_word_comb_reuse,
            MAX(c.cosine_max) AS cosine_max,
            AVG(c.cosine_max) AS avg_cosine_max,
            AVG(c.cosine_avg) AS avg_cosine_avg,
            SUM(c.n_words) AS n_words,
            SUM(c.n_bigrams) AS n_bigrams,
            SUM(c.n_trigrams) AS n_trigrams 
    FROM PaperAuthorUnique a
    INNER JOIN (
        SELECT AuthorId
        FROM current_authors
    ) USING(AuthorId)
    INNER JOIN (
        SELECT PaperId, AuthorId
        FROM (
            SELECT PaperId, AuthorId, AuthorSequenceNumber,
                   ROW_NUMBER() OVER (PARTITION BY PaperId ORDER BY AuthorSequenceNumber DESC) as rn
            FROM PaperAuthorAffiliations
        ) ranked
        WHERE rn = 1
    ) e USING(AuthorId, PaperId)
    INNER JOIN (
        SELECT PaperId, Year
        FROM Papers 
        WHERE 
            DocType IN ({insert_questionmark_doctypes}) 
            AND 
            DocType IS NOT NULL 
    ) d USING (PaperId)
    INNER JOIN paper_outcomes b USING(PaperId) 
    LEFT JOIN novelty_reuse c USING(PaperId)
    GROUP BY a.AuthorId, d.Year
""",
keep_doctypes
)

con.execute("CREATE UNIQUE INDEX idx_aol_AuthorIdYear on author_output_lastauthor (AuthorId ASC, Year)")

con.execute("""
    CREATE TABLE author_output AS 
    SELECT a.AuthorId, a.Year
        , a.PaperCount 
        , a.TotalAuthorCount
        , a.TotalForwardCitations
        , b.PaperCount AS PaperCount_firstauthor
        , b.TotalAuthorCount AS TotalAuthorCount_firstauthor
        , b.TotalForwardCitations AS TotalForwardCitations_firstauthor
        , c.PaperCount AS PaperCount_lastauthor
        , c.TotalAuthorCount AS TotalAuthorCount_lastauthor
        , c.TotalForwardCitations AS TotalForwardCitations_lastauthor
        , a.new_word 
        , a.new_word_reuse
        , a.new_bigram
        , a.new_bigram_reuse
        , a.new_trigram
        , a.new_trigram_reuse
        , a.new_word_comb
        , a.new_word_comb_reuse
        , a.cosine_max
        , a.avg_cosine_max
        , a.avg_cosine_avg
        , a.n_words
        , a.n_bigrams
        , a.n_trigrams
        , b.new_word AS new_word_firstauthor 
        , b.new_word_reuse AS new_word_reuse_firstauthor
        , b.new_bigram AS new_bigram_firstauthor
        , b.new_bigram_reuse AS new_bigram_reuse_firstauthor
        , b.new_trigram AS new_trigram_firstauthor
        , b.new_trigram_reuse AS new_trigram_reuse_firstauthor
        , b.new_word_comb AS new_word_comb_firstauthor
        , b.new_word_comb_reuse AS new_word_comb_reuse_firstauthor
        , b.cosine_max AS cosine_max_firstauthor
        , b.avg_cosine_max AS avg_cosine_max_firstauthor
        , b.avg_cosine_avg AS avg_cosine_avg_firstauthor
        , b.n_words AS n_words_firstauthor
        , b.n_bigrams AS n_bigrams_firstauthor
        , b.n_trigrams AS n_trigrams_firstauthor
        , c.PaperCount_english
        , c.TotalAuthorCount_english
        , c.TotalForwardCitations_english
        , c.new_word_english
        , c.new_word_reuse_english
        , c.new_bigram_english
        , c.new_bigram_reuse_english
        , c.new_trigram_english
        , c.new_trigram_reuse_english
        , c.new_word_comb_english
        , c.new_word_comb_reuse_english
        , c.cosine_max_english
        , c.avg_cosine_max_english
        , c.avg_cosine_avg_english
        , c.n_words_english
        , c.n_bigrams_english
        , c.n_trigrams_english
        , d.PaperCount AS PaperCount_mainpaper
        , d.TotalAuthorCount AS TotalAuthorCount_mainpaper
        , d.TotalForwardCitations AS TotalForwardCitations_mainpaper
        , d.new_word AS new_word_mainpaper
        , d.new_word_reuse AS new_word_reuse_mainpaper
        , d.new_bigram AS new_bigram_mainpaper
        , d.new_bigram_reuse AS new_bigram_reuse_mainpaper
        , d.new_trigram AS new_trigram_mainpaper
        , d.new_trigram_reuse AS new_trigram_reuse_mainpaper
        , d.new_word_comb AS new_word_comb_mainpaper
        , d.new_word_comb_reuse AS new_word_comb_reuse_mainpaper
        , d.cosine_max AS cosine_max_mainpaper
        , d.avg_cosine_max AS avg_cosine_max_mainpaper
        , d.avg_cosine_avg AS avg_cosine_avg_mainpaper
        , d.n_words AS n_words_mainpaper
        , d.n_bigrams AS n_bigrams_mainpaper
        , d.n_trigrams AS n_trigrams_mainpaper
    FROM author_output_total AS a
    LEFT JOIN author_output_firstauthor AS b
    USING(AuthorId, Year)
    LEFT JOIN author_output_total_english AS c
    USING(AuthorId, Year)
    LEFT JOIN author_output_mainpaper AS d
    USING(AuthorId, Year)
    """) # LEFT JOIN because there is years without first authorship, but any year with first authorship must be a year with any authorship.

con.execute("CREATE UNIQUE INDEX idx_ao_AuthorIdYear on author_output (AuthorId ASC, Year)")


print_elapsed_time(start_time)


# ## (6) Run ANALYZE, finish 
analyze_db(con)
con.close()

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")
