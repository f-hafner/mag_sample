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
--dry-run            Run the script with only 1 batch for testing
"""

import argparse
import numpy as np
import sqlite3 as sqlite
import pandas as pd
from pickle import load
import logging
import time
import os
import shutil
from scipy.sparse import csr_matrix

from helpers.variables import db_file, insert_questionmark_doctypes, keep_doctypes
from main.link.fit_svd_model import make_sparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def parse_args():
    parser = argparse.ArgumentParser(description='Apply SVD model to all Papers in the database')
    parser.add_argument("--model-path", type=str, default="/mnt/ssd/AcademicGraph/svd_model.pkl",
                        help="Path to the saved SVD model")
    parser.add_argument("--batch-size", type=int, default=100000,
                        help="Number of papers to process in each batch")
    parser.add_argument("--max-level", type=int, default=2,
                        help="Maximum level of fields of study to use")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run the script with only 1 batch for testing")
    parser.add_argument("--output-dir", type=str, default="svd_temp",
                        help="Directory to store output CSV files")
    return parser.parse_args()

def prepare_tables(con, max_level, dry_run=False):
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
        FROM Papers p
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

def create_output_directory(output_dir):
    "Create output directory, deleting it first if it already exists"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
        logging.info(f"Deleted existing output directory: {output_dir}")
    os.makedirs(output_dir)
    logging.info(f"Created new output directory: {output_dir}")

def write_embeddings_to_csv(paper_ids, embeddings, output_dir, batch_num):
    "Write embeddings to a CSV file"
    n_components = embeddings.shape[1]
    columns = ["PaperId"] + [f"dim_{i}" for i in range(n_components)]
    
    df = pd.DataFrame(np.column_stack([paper_ids, embeddings]), columns=columns)
    output_file = os.path.join(output_dir, f"embeddings_batch_{batch_num}.csv")
    df.to_csv(output_file, index=False)
    logging.info(f"Wrote embeddings to {output_file}")

def main(args):
    sqlite.register_adapter(np.int64, lambda val: int(val))
    con = sqlite.connect(database=db_file, isolation_level=None)
    
    start_time = time.time()
    
    prepare_tables(con, args.max_level, args.dry_run)
    svd_model = load_model(args.model_path)
    field_to_index = get_field_to_index(con)
    
    create_output_directory(args.output_dir)
    
    offset = 0
    total_papers = con.execute(f"SELECT COUNT(*) FROM Papers").fetchone()[0]
    
    processed_papers = 0
    batch_num = 0
    if args.dry_run: # run only 1 batch
        embeddings, paper_ids = process_batch(con, svd_model, field_to_index, args.batch_size, offset)        
        write_embeddings_to_csv(paper_ids, embeddings, args.output_dir, batch_num)
        processed_papers += len(paper_ids)  
    else: # run for all papers
        while True:
            embeddings, paper_ids = process_batch(con, svd_model, field_to_index, args.batch_size, offset)
            
            if embeddings is None:
                break
            
            write_embeddings_to_csv(paper_ids, embeddings, args.output_dir, batch_num)
            
            offset += args.batch_size
            processed_papers += len(paper_ids)
            batch_num += 1
            elapsed_time = time.time() - start_time
            logging.info(f"Processed {processed_papers}/{total_papers} papers. Elapsed time: {elapsed_time:.2f} seconds")
        
    total_time = time.time() - start_time
    logging.info(f"Finished processing all {processed_papers} papers in {total_time:.2f} seconds")

    con.close()

if __name__ == "__main__":
    args = parse_args()
    main(args)
