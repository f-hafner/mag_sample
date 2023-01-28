#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Script prep_quantiles_papercites.py
    Use multiprocessing to calculate 
    quantiles of citation distributions by field-Year
    and save as csv files.
    Field is at level 0 or 1 in MAG.
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
import numpy as np
import logging 
import itertools 


from helpers.variables import db_file, keep_doctypes_citations, insert_questionmark_doctypes_citations

logging.basicConfig(level=logging.INFO)


def enumerated_arguments(*args):
    "from a generator *args, yield a tuple (i, *args[i]) for i in range(len(args))."
    for i, k in enumerate(*args):
        yield ((i,) + tuple([j for j in k]))


def calculate_quantiles(
    chunk_id, 
    write_dir, 
    field, 
    year,
    level, 
    quantiles=list(np.arange(0, 1, 0.01))
    ):
    """
    For a year-field combination, calculate the quantiles of the 
    10 year forward citation distribution and save as csv. 

    Parameters
    ----------
    write_dir: str
       Directory to write the file to 
    chunk_id: int 
       Identifier of the chunk
    fields: list 
       Fields to process
    years: list
       years to process
    level: int
        Level of FieldOfStudyId, either 0 or 1.
    """
    # qmarks_years = ",".join(["?" for _ in range(len(years))])
    # qmarks_fields = ",".join(["?" for _ in range(len(fields))])
    qmarks_year = "?"
    qmarks_field = "?"
    field_column = f"Field{level}"
    sql = f"""
        SELECT a.PaperId
            , a.Year
            , b.CitationCount_y10
            , c.{field_column}
        FROM Papers AS a
        INNER JOIN 
        paper_outcomes AS b
        USING(PaperId)
        INNER JOIN 
        PaperMainFieldsOfStudy AS c
        USING(PaperId)
        WHERE a.DocType IN ({insert_questionmark_doctypes_citations})
        AND a.Year IN ({qmarks_year})
        AND c.{field_column} IN ({qmarks_field})
    """
    df = pd.read_sql(con=con, sql=sql, params=(list(keep_doctypes_citations) + [year] + [field]))
    logging.debug(f"df.shape is {df.shape}")
    logging.debug(f"year: {year}, field: {field}")
    if df.shape[0] > 0: # some field-year combinations may not exist in data.
        q_out = (df.groupby(["Year", field_column])["CitationCount_y10"]
                .quantile(q=quantiles)
                .reset_index()
                .rename(columns={"level_2": "quantile", 
                                "CitationCount_y10": "value"})
                )
        q_out["variable"] = "CitationCount_y10"
        logging.debug(f"current directory is {os.getcwd()}")
        logging.debug(f"write_dir is {args.write_dir}")
        q_out.to_csv(f"{write_dir}/part-{chunk_id}.csv", index=False)

# ## Arguments
parser = argparse.ArgumentParser(description = 'Inputs for prep_quantiles_papercites')
parser.add_argument("--nfields", 
                    dest="n_fields", 
                    default=1, 
                    help="Number of authors process. 'all' or an integer.") 
parser.add_argument("--level",
                    dest="level",
                    type=int,
                    default=1,
                    choices=[0, 1],
                    help="summarise at field level 0 or 1?")
# parser.add_argument("--chunksize", 
#                     dest = "chunk_size",
#                     default = 10_000,
#                     type = int,
#                     help = "Number of authors to process in one chunk")  
parser.add_argument("--ncores", 
                    dest = "n_cores", 
                    default = int(mp.cpu_count() / 2),
                    type = int, help = "Number of cores to use")
parser.add_argument("--write_dir", 
                    dest = "write_dir", 
                    default = "quantiles_temp/")
parser.add_argument("--start_year",
                    dest="start_year",
                    type=int,
                    default=1970)
parser.add_argument("--end_year", 
                    dest="end_year",
                    type=int,
                    default=2020)

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

## Prepare inputs for map
if args.n_fields == "all":
    args.n_fields = con.execute(f"SELECT COUNT(FieldOfStudyId) FROM FieldsOfStudy WHERE Level = {args.level}").fetchall()[0][0]
else:
    args.n_fields = int(args.n_fields)

query = "SELECT DISTINCT FieldOfStudyId FROM FieldsOfStudy WHERE Level = ? LIMIT ?"
fields = pd.read_sql(sql=query, con=con, params=(args.level, args.n_fields))


years = [y for y in range(args.start_year, args.end_year)]
inputs = itertools.product(
    [args.write_dir], fields.FieldOfStudyId, years, [args.level]
    )

# ## Map 
enumerated_inputs = enumerated_arguments(inputs)
print("Running queries...", flush=True)
if __name__ == "__main__":
    with Pool(processes = args.n_cores) as pool:
        results = pool.starmap(calculate_quantiles, enumerated_inputs)
    print("--queries finished.")


# ## Finish
con.close()
end_time = time.time()

print(f"Done in {(end_time - start_time)/60} minutes.")
