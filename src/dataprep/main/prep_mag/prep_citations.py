#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script prep_citations.py

Generate tables
- paper_citations: citation history for each paper, by ReferencingDocType x Year

"""


import sqlite3 as sqlite
import time 

from helpers.variables import db_file, keep_doctypes_citations, insert_questionmark_doctypes_citations

# ## Arguments


# ## Variables; connect to db
start_time = time.time()
print(f"Start time: {start_time} \n")

start_year = 1950 
con = sqlite.connect(database = db_file, isolation_level= None)


# ## Citation history per paper 
print('Citations per Paper-Year... \n')
con.execute('DROP TABLE IF EXISTS paper_citations')
con.execute(f"""CREATE TABLE paper_citations AS
            SELECT  a.PaperReferenceId, 
                    b.Year, 
                    b.DocType AS ReferencingDocType, 
                    COUNT(DISTINCT a.PaperId) AS CitationCount 
            FROM PaperReferences a 
            -- ## Restrictions on referencing papers
            INNER JOIN (
              SELECT PaperId, Year, DocType 
              FROM Papers 
              INNER JOIN (
                  SELECT PaperId 
                  FROM PaperMainFieldsOfStudy
              ) USING (PaperId)
              WHERE 
                DocType IN ({insert_questionmark_doctypes_citations}) 
                AND 
                DocType IS NOT NULL 
            ) b on a.PaperId = b.PaperId 
            -- ## Restrictions on PaperReferenceId 
            INNER JOIN (
              SELECT PaperId, Year, DocType 
              FROM Papers 
              INNER JOIN ( -- ## this keeps only the citation history of papers in PaperMainFieldsOfStudy
                  SELECT PaperId 
                  FROM PaperMainFieldsOfStudy
              ) USING (PaperId) 
              WHERE 
                DocType IN ({insert_questionmark_doctypes_citations}) 
                AND 
                DocType IS NOT NULL 
                AND 
                Year >= (?)
            ) c on a.PaperReferenceId = c.PaperId 
            GROUP BY a.PaperReferenceId, b.Year, ReferencingDocType
          """,
          (keep_doctypes_citations + keep_doctypes_citations + (start_year,))
          )

con.execute('CREATE INDEX idx_pc_PaperReferenceIdYear on paper_citations (PaperReferenceId ASC, Year)')


# ## Run ANALYZE, finish
print("Running ANALYZE... \n")
cursor = con.cursor()
cursor.execute("PRAGMA analysis_limit = 1000")
cursor.execute("PRAGMA optimize")

cursor.close()
con.close()

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")