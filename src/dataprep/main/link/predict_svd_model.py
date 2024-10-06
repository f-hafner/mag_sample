"""
Apply SVD model to all Papers in the database

This script loads the SVD model created by fit_svd_model.py and applies it to all Papers in the database.
It creates a new table in the database with the SVD embeddings for each paper.

Usage:
python predict_svd_model.py [options]

Options:
--model-path STR     Path to the saved SVD model (default: "/mnt/ssd/AcademicGraph/svd_model.pkl")
--batch-size INT     Number of papers to process in each batch (default: 100000)
--max-level INT      Maximum level of fields of study to use (default: 2)
--start INT          Minimum publication year of papers to consider (default: 1980)
--end INT            Maximum publication year of papers to consider (default: 2020)
"""

import argparse
import numpy as np
import sqlite3 as sqlite
import pandas as pd
from pickle import load
import logging
from scipy.sparse import csr_matrix
from tqdm import tqdm

from helpers.variables import db_file, insert_questionmark_doctypes, keep_doctypes
from fit_svd_model import make_sparse

logging.basicConfig(level=logging.INFO)

def parse_args():
    parser = argparse.ArgumentParser(description='Apply SVD model to all Papers in the database')
    parser.add_argument("--model-path", type=str, default="/mnt/ssd/AcademicGraph/svd_model.pkl",
                        help="Path to the saved SVD model")
    parser.add_argument("--batch-size", type=int, default=100000,
                        help="Number of papers to process in each batch")
    parser.add_argument("--max-level", type=int, default=2,
                        help="Maximum level of fields of study to use")
    parser.add_argument("--start", type=int, default=1980,
                        help="Minimum publication year of papers to consider")
    parser.add_argument("--end", type=int, default=2020,
                        help="Maximum publication year of papers to consider")
    return parser.parse_args()

def prepare_tables(con, start_year, end_year, max_level):
    "Prepare tables for loading papers and fields"
    
    logging.info("Creating fields_to_max_level table")
    con.execute(
        """CREATE TEMP TABLE IF NOT EXISTS fields_to_max_level AS
        SELECT FieldOfStudyId
        FROM FieldsOfStudy
        WHERE Level > 0 AND Level <= (?)
        """, (max_level,)
    )
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_temp1 ON fields_to_max_level(FieldOfStudyId ASC)")

    logging.info("Creating valid_papers table")
    sql_valid_papers = f"""CREATE TABLE IF NOT EXISTS valid_papers AS
        SELECT PaperId
        FROM Papers
        WHERE Year >= (?)
        AND Year <= (?)
        AND DocType IN ({insert_questionmark_doctypes})"""
    
    con.execute(sql_valid_papers, (start_year, end_year, *keep_doctypes))
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_valid_papers ON valid_papers(PaperId)")

def load_model(model_path):
    "Load the SVD model from file"
    logging.info(f"Loading SVD model from {model_path}")
    with open(model_path, "rb") as f:
        return load(f)

def get_field_to_index(con):
    "Get mapping of FieldOfStudyId to column index"
    fields_of_study = pd.read_sql("SELECT * FROM fields_to_max_level ORDER BY FieldOfStudyId", con=con)
    return {id: index for index, id in enumerate(fields_of_study['FieldOfStudyId'].unique())}

def process_batch(con, svd_model, field_to_index, batch_size, offset):
    "Process a batch of papers and return their SVD embeddings"
    
    sql_load_papers = f"""
        SELECT p.PaperId, pf.FieldOfStudyId, pf.Score
        FROM valid_papers p
        INNER JOIN PaperFieldsOfStudy pf ON p.PaperId = pf.PaperId
        INNER JOIN fields_to_max_level f ON pf.FieldOfStudyId = f.FieldOfStudyId
        LIMIT {batch_size} OFFSET {offset}
    """
    
    papers_fields = pd.read_sql(sql_load_papers, con=con)
    
    if papers_fields.empty:
        return None, None
    
    papers_fields_sparse, row_to_index = make_sparse(
        papers_fields, field_to_index, "PaperId", "FieldOfStudyId", "Score")
    
    embeddings = svd_model.transform(papers_fields_sparse)
    
    return embeddings, list(row_to_index.keys())

def create_embeddings_table(con, n_components):
    "Create table to store paper embeddings"
    columns = ", ".join([f"dim_{i} REAL" for i in range(n_components)])
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS paper_embeddings (
        PaperId INTEGER PRIMARY KEY,
        {columns}
    )
    """)

def insert_embeddings(con, paper_ids, embeddings):
    "Insert embeddings into the database"
    n_components = embeddings.shape[1]
    placeholders = ", ".join(["?" for _ in range(n_components + 1)])
    sql = f"INSERT OR REPLACE INTO paper_embeddings VALUES ({placeholders})"
    
    data = [tuple([paper_id] + embedding.tolist()) for paper_id, embedding in zip(paper_ids, embeddings)]
    con.executemany(sql, data)

def main(args):
    sqlite.register_adapter(np.int64, lambda val: int(val))
    con = sqlite.connect(database=db_file, isolation_level=None)
    
    prepare_tables(con, args.start, args.end, args.max_level)
    svd_model = load_model(args.model_path)
    field_to_index = get_field_to_index(con)
    
    create_embeddings_table(con, svd_model.n_components_)
    
    offset = 0
    total_papers = con.execute("SELECT COUNT(*) FROM valid_papers").fetchone()[0]
    
    with tqdm(total=total_papers, desc="Processing papers") as pbar:
        while True:
            embeddings, paper_ids = process_batch(con, svd_model, field_to_index, args.batch_size, offset)
            
            if embeddings is None:
                break
            
            insert_embeddings(con, paper_ids, embeddings)
            
            offset += args.batch_size
            pbar.update(len(paper_ids))
    
    logging.info("Finished processing all papers")
    con.close()

if __name__ == "__main__":
    args = parse_args()
    main(args)
