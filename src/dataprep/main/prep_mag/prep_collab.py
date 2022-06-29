#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Script prep_collab.py
    Use multiprocessing to extract unique 
    author-coauthor-year combinations and save 
    as csv files
"""

import multiprocessing as mp
import sqlite3 as sqlite
import time 
import argparse
import pandas as pd 
from multiprocessing import Pool
import math 
import os 
import sys 

from helpers.functions import print_elapsed_time, analyze_db
from helpers.variables import db_file, keep_doctypes, insert_questionmark_doctypes

def get_coauthors(write_dir, chunk_id, authors):
    """
    For a set of authors, extract coauthors for each year.

    Parameters
    ----------
    write_dir: str
        Directory to write the file to.
    chunk_id: int
        Identifier of the chunk.
    authors: list
        Authors to process.
    """
    qmarks = ",".join(["?" for i in range(len(authors))])
    sql = f"""
    SELECT a.AuthorId, d.AuthorId AS CoAuthorId, d.Year 
        FROM PaperAuthorUnique AS a
        INNER JOIN (
            SELECT b.*, c.Year 
            FROM PaperAuthorUnique AS b
            INNER JOIN (
                SELECT PaperId, Year 
                FROM Papers
                WHERE DocType IN ({insert_questionmark_doctypes})
            ) AS c
            USING (PaperId)
        ) AS d
        ON (a.PaperId = d.PaperId and a.AuthorId != d.AuthorId)
        -- drop authors not in author_sample
        INNER JOIN (
            SELECT AuthorId
            FROM author_sample
            WHERE AuthorId IN ({qmarks})
        ) AS e ON (a.AuthorId = e.AuthorId)
        INNER JOIN (
            SELECT AuthorId
            FROM author_sample
        ) AS f on (CoAuthorId = f.AuthorId)
    """
    df = pd.read_sql(con = con, sql = sql, params = (list(keep_doctypes) + authors))
    df = df.drop_duplicates()
    df.to_csv(f"{write_dir}/part-{chunk_id}.csv", index = False)

# ## Arguments
parser = argparse.ArgumentParser(description = 'Inputs for author_collab')
parser.add_argument("--nauthors", 
                    dest = "n_authors", 
                    default = 100_000, 
                    help = "Number of authors process. 'all' or an integer.") 
parser.add_argument("--chunksize", 
                    dest = "chunk_size",
                    default = 10_000,
                    type = int,
                    help = "Number of authors to process in one chunk")  
parser.add_argument("--ncores", 
                    dest = "n_cores", 
                    default = int(mp.cpu_count() / 2),
                    type = int, help = "Number of cores to use")
parser.add_argument("--write_dir", 
                    dest = "write_dir", 
                    default = "collab_temp/")

args = parser.parse_args()

if os.path.isdir(args.write_dir):
    sys.exit("You specified an existing directory.")

if args.n_cores > mp.cpu_count():
    print("Specified too many cpu cores.")
    print(f"Using max available, which is {mp.cpu_count()}.")
    args.n_cores = mp.cpu_count()

# ## Setup
start_time = time.time()
print(f"Start time: {start_time} \n")
os.mkdir(args.write_dir)

con = sqlite.connect(database = "file:" + db_file + "?mode=ro", 
                     isolation_level = None, uri = True) # read-only connection 

# ## Prepare inputs for map
if args.n_authors == "all":
    args.n_authors = con.execute("SELECT COUNT(AuthorId) FROM author_sample").fetchall()[0][0]
else:
    args.n_authors = int(args.n_authors)

query = "SELECT DISTINCT AuthorId from author_sample LIMIT ?" 
authors = pd.read_sql(sql = query, con = con, params = (args.n_authors,))

# n_authors = authors.shape[0] 
n_groups = math.ceil(args.n_authors / args.chunk_size)
list_in = ([(args.write_dir, 
             i, 
             (authors.AuthorId
                .iloc[range(i * args.chunk_size, 
                            min(i * args.chunk_size + args.chunk_size,
                                args.n_authors))
                     ]
                .tolist())
            ) 
             for i in range(n_groups)]
          )

# ## Map 
print("Running queries...", flush = True)
if __name__ == "__main__":
    with Pool(processes = args.n_cores) as pool:
        results = pool.starmap(get_coauthors, list_in)
    print("--queries finished.")


# ## Finish
con.close()
end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")


# ## To do it sequentially:
# from itertools import starmap
# d = starmap(get_coauthors, list_in)
# [print(i) for i in d]