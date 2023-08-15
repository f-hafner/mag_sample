#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Script venue_citations.py

Generate tables
- journal_citations: 10-year forward citations of journals, by Year.
- conference_citations: 10-year forward citations of conferences, by Year.

"""


import sqlite3 as sqlite
import time 

from helpers.variables import db_file
from helpers.functions import analyze_db

# ## Arguments

# ## Variables; connect to db
start_time = time.time()
print(f"Start time: {start_time} \n")

start_year = 1950 
con = sqlite.connect(database = db_file, isolation_level= None)


# ## Citation history per paper 
print('Aggregating citations and venue level... \n')

map_dict = {
    "journal_citations": {
        "DocType": "Journal",
        "identifier": "JournalId",
        "index_name": "idx_jc_JournalIdYear"
    },
    "conference_citations": {
        "DocType": "Conference",
        "identifier": "ConferenceSeriesId",
        "index_name": "idx_cc_ConferenceSeriesIdYear"
    }
}

for tbl_name, tbl_info in map_dict.items():
    insert_values = (tbl_info['DocType'], start_year)
    index_query = f"CREATE UNIQUE INDEX {tbl_info['index_name']} ON {tbl_name} ({tbl_info['identifier']}, Year)"

    with con as c:
        c.execute(f"DROP TABLE IF EXISTS {tbl_name}")
        
        c.execute(f"""CREATE TABLE {tbl_name} AS 
        SELECT {tbl_info['identifier']}
            , Year
            , AVG(CitationCount_y10) AS Avg_CitationCount_y10
        FROM paper_outcomes
        INNER JOIN (
            SELECT PaperId
                , {tbl_info['identifier']}
                , Year 
            FROM Papers
            WHERE DocType = ?
            AND Year >= ?
        ) USING(PaperId)
        GROUP BY {tbl_info['identifier']}, Year
        """, insert_values )

        c.execute(index_query)


with con as c:
    analyze_db(c)


con.close()

end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")
