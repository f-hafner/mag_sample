"""
Write links to database with selected minimum scores.
"""

import sqlite3 as sqlite
import argparse
import os
import pandas as pd
import pdb

from helpers.variables import db_file
from helpers.functions import analyze_db


parser = argparse.ArgumentParser()
parser.add_argument("--fromdir", 
                    type=str,
                    default="../../data/link_institutions/",
                    help="file path from where to read the links from") 
parser.add_argument("--minmag", 
                    type=float,
                    default=0.6,
                    help="minimum score for mag links") 
parser.add_argument("--minpq", 
                    type=float,
                    default=0.3,
                    help="minimum score for proquest links") 
args = parser.parse_args()


link_files = os.listdir(args.fromdir)
links = []
for f in link_files:
    links.append(pd.read_csv(args.fromdir + f))

links = pd.concat(links)

mask_mag = (links["from_dataset"] == "mag") \
    & (links["link_score"] >= args.minmag)
mask_pq = (links["from_dataset"] == "pq") \
    & (links["link_score"] >= args.minpq)
mask = (mask_mag) | (mask_pq)
links = links.loc[mask, :]

links = links.astype({"from_id": "int64", "unitid": "int64"})

con = sqlite.connect(db_file)
with con: 
    c = con.cursor()
    c.execute("DROP TABLE IF EXISTS links_to_cng")

    # write to db
    links.to_sql("links_to_cng", 
                con=con, 
                index=False, 
                chunksize=links.shape[0]
                )

    con.execute("CREATE INDEX idx_ltc_fromidunitid ON links_to_cng(from_id ASC, unitid ASC)")
    con.execute("CREATE INDEX idx_ltc_unitid ON links_to_cng(unitid ASC)")

    analyze_db(con)

con.close()

print("Done.")