#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script paper_outcomes.py

Generate a table with paper variables:
- number of authors per paper 
- citation measures: number of citations in first x years
"""

import sqlite3 as sqlite
import warnings
import time 
import argparse
from helpers.functions import print_elapsed_time, analyze_db
from helpers.variables import db_file


# ## Arguments


# ## Variables; connect to db
start_time = time.time()
print(f"Start time: {start_time} \n")

con = sqlite.connect(database = db_file, isolation_level= None)


# ## team size by paper 
print_elapsed_time(start_time)
print("Making paper_teamsize... \n")
con.execute('DROP TABLE IF EXISTS paper_teamsize')
con.execute('''CREATE TEMPORARY TABLE paper_teamsize AS 
                  SELECT PaperId, 
                         COUNT(AuthorId) AS AuthorCount
                  FROM PaperAuthorUnique  
                  GROUP BY PaperId
                ''')
con.execute("CREATE UNIQUE INDEX idx_pa_PaperId on paper_teamsize (PaperId ASC)")

# ## Citation measures
tablequery="""CREATE TEMPORARY TABLE citationtemp AS
    SELECT a.PaperReferenceId AS PaperId,
           SUM(a.CitationCount) AS CitationCount_y10,
        b.Year as YearPub
    FROM Papers b
    LEFT JOIN paper_citations a ON b.PaperId = a.PaperReferenceId
        WHERE ReferencingDocType IS NOT NULL 
        AND ReferencingDocType IN ("Journal", "Book", "BookChapter", "Conference", "Thesis")
        AND  a.Year-b.Year <= 10
    GROUP BY a.PaperReferenceId
"""

tablequery2="""CREATE TEMPORARY TABLE citationcount10 AS 
    SELECT a.PaperId, 
        CASE WHEN b.CitationCount_y10 IS NULL THEN 0 ELSE b.CitationCount_y10 END
        AS CitationCount_y10
    FROM Papers a  
    LEFT JOIN citationtemp b USING(PaperId)
 """

con.execute("""DROP TABLE IF EXISTS citationtemp""")
con.execute(tablequery)
con.execute("CREATE UNIQUE INDEX id_paper_citetemp on citationtemp (PaperId ASC)")

con.execute("""DROP TABLE IF EXISTS citationcount10""")
con.execute(tablequery2)
con.execute("CREATE UNIQUE INDEX id_paper_cite10 on citationcount10 (PaperId ASC)")

## Now repeat the same for 5 years.

tablequery="""CREATE TEMPORARY TABLE citationtemp AS
    SELECT a.PaperReferenceId AS PaperId,
           SUM(a.CitationCount) AS CitationCount_y5,
        b.Year as YearPub
    FROM Papers b
    LEFT JOIN paper_citations a ON b.PaperId = a.PaperReferenceId
        WHERE ReferencingDocType IS NOT NULL 
        AND ReferencingDocType IN ("Journal", "Book", "BookChapter", "Conference", "Thesis")
        AND  a.Year-b.Year <= 5
    GROUP BY a.PaperReferenceId
"""

tablequery2="""CREATE TEMPORARY TABLE citationcount5 AS 
    SELECT a.PaperId, 
        CASE WHEN b.CitationCount_y5 IS NULL THEN 0 ELSE b.CitationCount_y5 END
        AS CitationCount_y5
    FROM Papers a  
    LEFT JOIN citationtemp b USING(PaperId)
 """
con.execute("""DROP TABLE IF EXISTS citationtemp""")
con.execute(tablequery)
con.execute("CREATE UNIQUE INDEX id_paper_citetemp on citationtemp (PaperId ASC)")

con.execute("""DROP TABLE IF EXISTS citationcount5""")
con.execute(tablequery2)
con.execute("CREATE UNIQUE INDEX id_paper_cite5 on citationcount5 (PaperId ASC)")



# ## Final table 
con.execute("DROP TABLE IF EXISTS paper_outcomes")
con.execute("""CREATE TABLE paper_outcomes AS 
            SELECT a.PaperId, a.CitationCount_y10, b.CitationCount_y5, c.AuthorCount
            FROM citationcount10 a
            INNER JOIN citationcount5 b USING (PaperId)
            INNER JOIN paper_teamsize c USING (PaperId)
""")

con.execute("CREATE UNIQUE INDEX idx_po_PaperId on paper_outcomes (PaperId ASC)")


# ## Run ANALYZE, finish
analyze_db(con)

con.close()

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")




