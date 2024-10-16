"""Prepare data and fit truncated SVD on subsample of papers

This script performs truncated SVD (Singular Value Decomposition) on a subsample of academic papers from a SQLite database.

The script does the following:
1. Prepares temporary tables in the database:
   - 'fields_to_max_level': Contains FieldOfStudyIds up to a specified maximum level.
   - 'valid_papers': Persistent table with papers matching specified criteria (year range and document types).
   - 'selected_papers_svd': Temporary table with a random sample of papers for SVD analysis.

2. Loads data for papers and their associated fields of study.
3. Creates a sparse matrix representation of the paper-field relationships.
4. Performs truncated SVD on this matrix.
5. Saves the resulting SVD model to a file.

Usage:
python script_name.py [options]

Options:
--ndim INT           Number of dimensions for reduced concept vectors (default: 1024)
--max-level INT      Maximum level of fields of study to use (default: 2)
--start INT          Minimum publication year of papers to consider (default: 1980)
--end INT            Maximum publication year of papers to consider (default: 2020)
--dry-run            Run the script with a small subset of data for testing

Constants:
- SAMPLE_SIZE: number of papers to fit the SVD model on.  
- MODEL_URL: location to save the model
- RANDOM_SEED

Note: The 'valid_papers' table is the only persistent table created by this script. 
All other tables are temporary and will be deleted when the database connection is closed.
"""

import argparse
import numpy as np 
import sqlite3 as sqlite
import pandas as pd 
from pickle import dump
import logging

from scipy.sparse import csr_matrix  
from sklearn.decomposition import TruncatedSVD  
from helpers.variables import db_file, insert_questionmark_doctypes, keep_doctypes


logging.basicConfig(level=logging.INFO)


SAMPLE_SIZE = 5_000_000
MODEL_URL = "/mnt/ssd/AcademicGraph/svd_model"
RANDOM_SEED = 58352


def make_sparse(long_df, field_to_index, rows="AffiliationId", cols="FieldOfStudyId", value_col="score"):
    """Create sparse matrix from a dataframe.

    Args:
        long_df (pd.DataFrame): the data to be converted.
        field_to_index (dict): exhaustive mapping of field of study id to column 
        index in the resulting matrix.
        rows (str): column name in `long_df` to be converted to rows.
        cols (str): column name in `long_df` to be converted to columns.
        value_col (str): column name in `long_df` for non-zero elements in the 
        sparse matrix.

    Returns:
        tuple: (sparse matrix, map from row IDs in dataframe to row index in the sparse matrix)
    """ 

    row_values = np.array(long_df[rows])
    col_values = np.array(long_df[cols])
    data = np.array(long_df[value_col])

    row_to_index = {id: index for index, id in enumerate(np.unique(row_values))}
    row_to_index_map = np.vectorize(row_to_index.get)
    field_to_index_map = np.vectorize(field_to_index.get)

    cols = field_to_index_map(col_values)
    rows = row_to_index_map(row_values) 

    out = csr_matrix((data, (rows, cols)),
                     shape=(len(row_to_index), len(field_to_index))
                    )
    print(data.shape)
    print(out.__repr__())

    return out, row_to_index


def run_svd(in_matrix, n_components=50, svd=None):
    """Compute truncated SVD on `in_matrix`

    Args:
        in_matrix: matrix where rows are samples and columns are features.
        n_components: dimension of the reduced feature space.
        svd (optional): sklearn.decomposition.TruncatedSVD. 

    If `svd` is not supplied, a new instance is created with the specified parameters.
    If `svd` is supplied, only `transform` is called on the input data.

    Returns:
        tuple: (svd model, embeddings in subspace)
    """
    fit = False
    if not svd:
        fit = True
        svd = TruncatedSVD(n_components=n_components, random_state=RANDOM_SEED)
    
    if fit: 
        embs = svd.fit_transform(in_matrix)
    else:
        embs = svd.transform(in_matrix)
    
    print(f"Original matrix shape: {in_matrix.shape}")
    print(f"Reduced matrix shape: {embs.shape}")
    print(f"Explained variance ratio: {svd.explained_variance_ratio_.sum():.4f}") 
    
    return svd, embs



def prepare_tables(con, start_year, end_year, max_level, dry_run=False):
    "Prepare temp tables for loading papers and fields"
    
    logging.info("making fields_to_max_level")
    con.execute(
            """CREATE TEMP TABLE fields_to_max_level AS
            SELECT FieldOfStudyId
            FROM FieldsOfStudy
            WHERE Level > 0 AND Level <= (?)
            """
            , (max_level,)
            )
    con.execute("CREATE UNIQUE INDEX idx_temp1 ON fields_to_max_level(FieldOfStudyId ASC)")

    logging.info("creating valid papers")

    con.execute("DROP TABLE IF EXISTS valid_papers")
    sql_valid_papers = f"""CREATE TABLE valid_papers AS
        SELECT PaperId
        FROM Papers
        WHERE Year >= (?)
        AND Year <= (?)
        AND DocType IN ({insert_questionmark_doctypes})"""
    
    if dry_run:
        sql_valid_papers += " LIMIT 1000"


    logging.debug(sql_valid_papers)

    con.execute(sql_valid_papers, (start_year, end_year, *keep_doctypes))


def sample_papers(con, dry_run=False):
    "Sample papers and write to database"
    
    generator = np.random.default_rng(RANDOM_SEED)
    all_papers = pd.read_sql("SELECT PaperId FROM valid_papers", con=con) 

    if dry_run:
        subsample = all_papers.sample(n=500, random_state=generator)
    else:
        subsample = all_papers.sample(n=SAMPLE_SIZE, random_state=generator)

    logging.info("Writing sampled papers")

    cursor = con.cursor()
    data_to_insert = [(x,) for x in subsample["PaperId"].values]
    # sql_delete = "DROP TABLE IF EXISTS selected_papers_svd"
    sql_create = "CREATE TEMP TABLE selected_papers_svd (PaperId INT)"

    cursor.execute(sql_create)
    cursor.executemany("INSERT INTO selected_papers_svd (PaperId) VALUES (?)", data_to_insert)
    cursor.execute("CREATE UNIQUE INDEX idx_paperid_selp ON selected_papers_svd(PaperId)")
    con.commit()


def load_data_for_svd(con):
    """Load data for SVD decomposition
        
    Returns:
        tuple: (dataframe with papers, dataframe with exhaustive FieldOfStudyIds) 
    """
    
    sql_load_papers = """
        SELECT PaperId, FieldOfStudyId, Score
        FROM valid_papers
        INNER JOIN PaperFieldsOfStudy
        USING (PaperId)
        INNER JOIN fields_to_max_level
        USING (FieldOfStudyId)
        INNER JOIN selected_papers_svd
        USING (PaperId)
    """

    papers_fields = pd.read_sql(sql_load_papers, con=con) 
    fields_of_study = pd.read_sql("SELECT * FROM fields_to_max_level ORDER BY FieldOfStudyId", con=con)
    
    return papers_fields, fields_of_study




def parse_args():
    parser = argparse.ArgumentParser(description = 'Run truncated SVD on subset of papers')
    parser.add_argument("--ndim", type=int, default=1024,
                        help="Number of dimension for reduced concept vectors")

    parser.add_argument("--max-level", type=int, default=2, dest="max_level",
                        help="Maximum level of fields of study and respective scores to use")

    parser.add_argument("--start", type=int, default=1980,
                        help="Minimum publication year of papers to consider.")

    parser.add_argument("--end", type=int, default=2020,
                        help="Maximum publication year of papers to consider.")

    parser.add_argument('--dry-run', action=argparse.BooleanOptionalAction, dest="dry_run")
   
    args = parser.parse_args()
    return args



def main(args):

    model_url = MODEL_URL
    sqlite.register_adapter(np.int64, lambda val: int(val))
 
    con = sqlite.connect(database=db_file, isolation_level=None)

    prepare_tables(con, args.start, args.end, args.max_level, args.dry_run)

    logging.info("loading valid papers")
    sample_papers(con, args.dry_run)
    
    
    logging.info("loading sampled papers and fields")
    papers_fields, fields_of_study = load_data_for_svd(con) 
    
    field_to_index = {id: index for index, id in enumerate(fields_of_study['FieldOfStudyId'].unique())}
   
    logging.info("making sparse matrix and running SVD")
    papers_fields_sparse, _ = make_sparse(
        papers_fields, field_to_index, "PaperId", "FieldOfStudyId", "Score")
    
    svd, _  = run_svd(papers_fields_sparse, args.ndim)
    
   
    logging.info("saving model")

    if args.dry_run:
        model_url += "_dry"
    
    with open(model_url + "_" + str(args.ndim) + ".pkl", "wb") as f:
        dump(svd, f, protocol=5)
    
    logging.info("Done.")


if __name__ == "__main__":
    args = parse_args()
    main(args)


