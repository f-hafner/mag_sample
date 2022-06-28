#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Script prep_collab.py

Generate tables:
- author_collab: a table with each unique co-author pairs for each year 

"""

import multiprocessing
import sqlite3 as sqlite
import time 
import argparse
import pandas as pd 
from multiprocessing import Pool
import multiprocessing
import math 
import os 
import sys 

from helpers.functions import print_elapsed_time, analyze_db
from helpers.variables import db_file 

def proc_grp(write_dir, proc_id, authors):
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
    df = pd.read_sql(con = con, sql = sql, params = authors)
    df = df.drop_duplicates()
    df.to_csv(f"{write_dir}/part-{proc_id}.csv", index = False)

# ## Arguments
parser = argparse.ArgumentParser(description = 'Inputs for author_collab')
parser.add_argument("--nauthors", dest="n_authors", default = 1_000_000) # todo: how to use "all" here? just pass "all". "LIMIT NULL" -> sqlite reads all
parser.add_argument("--chunksize", dest="chunk_size", default = 100_000) # this seems like a good default 
parser.add_argument("--ncores", dest = "n_cores", default = int(multiprocessing.cpu_count() / 2))
parser.add_argument("--write_dir", dest="write_dir", default = "collab_temp/")


args = parser.parse_args()

n_cores = args.n_cores
chunk_size = args.chunk_size
n_authors = args.n_authors
write_dir = args.write_dir

print("write_dir is", write_dir)

if os.path.isdir(write_dir):
    sys.exit("You specified an existing directory.")

os.mkdir(write_dir)

if n_cores > multiprocessing.cpu_count():
    print(f"Specified too many cpu cores. Using max available, which is {multiprocessing.cpu_count()}.")
    n_cores = multiprocessing.cpu_count()


# ## Variables; connect to db
start_time = time.time()
print(f"Start time: {start_time} \n")

con = sqlite.connect(database = "file:" + db_file + "?mode=ro", 
                     isolation_level = None, uri = True) # read-only connection 


# extract relevant authors 
query = "SELECT DISTINCT AuthorId from author_sample LIMIT ?" # TODO: how to do this when querying all authors?
authors = pd.read_sql(sql = query, con = con, params = (n_authors,))

# TODO
    # clear the directory with all the temp files 
    # coordinate args.n_authors with the code below also 
    # add argument for directory in fct; make sure directory is clean and exists before running the pooled stuff 
    # seems hard to predict whether a given chunk is large or small and needs a lot of filtering. but I guess this is not knowable ex-ante? 
    # fix the file description 
    # test with few authors -- does it run through?  then test with more, but not in a "live" session 
    # check imap? chunksize seems like it could make it faster? https://docs.python.org/3/library/multiprocessing.html#multiprocessing.pool.Pool.imap_unordered

# input for map
n_authors = authors.shape[0] 
n_groups = math.ceil(n_authors / chunk_size)
list_in = ([(write_dir, i, authors.AuthorId.iloc[range(i * chunk_size, min(i * chunk_size + chunk_size, n_authors) )].tolist()) for i in range(n_groups)])

# map 
print("Running queries...", flush = True)
if __name__ == "__main__":
    with Pool(processes = n_cores) as pool:
        results = pool.starmap(proc_grp, list_in)
    print("--queries finished.")


con.close()


end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")



# sequentially
# from itertools import starmap
# d = starmap(proc_grp, list_in)
# [print(i) for i in d]


