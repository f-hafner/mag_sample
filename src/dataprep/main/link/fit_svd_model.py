"""Prepare data and fit truncated SVD on subsample of papers"""


import numpy as np 
import sqlite3 as sqlite
import numpy as np      
import pandas as pd 
from pickle import dump
import logging

from scipy.sparse import csr_matrix                                                                                                                                                         
from sklearn.decomposition import TruncatedSVD  
from helpers.variables import db_file, insert_questionmark_doctypes, keep_doctypes


# TODO: valid_papers: should be persistent? -> for running the model later on

logging.basicConfig(level=logging.INFO)


N_DIM = 1024
SAMPLE_SIZE = 1_000_000
DRY_RUN = True
MODEL_URL = "/mnt/ssd/AcademicGraph/svd_model"


RANDOM_SEED = 583523592352
sample_size = 1_000_000

# TODO: is this correct? -- doublecheck with Christoph
max_level = 2
start_year = 1980
end_year = 2020



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


def run_svd(in_matrix, n_components=50):
    """Compute truncated SVD on `in_matrix`

    Args:
        in_matrix: matrix where rows are samples and columns are features.
        n_components: dimension of the reduced feature space.

    Returns:
        tuple: (svd model, embeddings in subspace)
    """
    svd = TruncatedSVD(n_components=n_components, random_state=42) 
    embs = svd.fit_transform(in_matrix)
    
    print(f"Original matrix shape: {in_matrix.shape}")
    print(f"Reduced matrix shape: {embs.shape}")
    print(f"Explained variance ratio: {svd.explained_variance_ratio_.sum():.4f}") 
    
    return svd, embs



def prepare_tables(con):
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
    
    if DRY_RUN:
        sql_valid_papers += " LIMIT 1000"


    logging.debug(sql_valid_papers)

    con.execute(sql_valid_papers, (start_year, end_year, *keep_doctypes))


def sample_papers(con):
    "Sample papers and write to database"
    
    generator = np.random.default_rng(RANDOM_SEED)
    
    all_papers = pd.read_sql("SELECT PaperId FROM valid_papers", con=con) 

    if DRY_RUN:
        subsample = all_papers.sample(n=500, random_state=generator)
    else:
        subsample = all_papers.sample(n=SAMPLE_SIZE, random_state=generator)

    logging.info("writing sampled papers")

    cursor = con.cursor()
    data_to_insert = [(x,) for x in subsample["PaperId"].values]
    # sql_delete = "DROP TABLE IF EXISTS selected_papers_svd"
    sql_create = "CREATE TEMP TABLE selected_papers_svd (PaperId INT)"

    cursor.execute(sql_create)
    cursor.executemany("INSERT INTO selected_papers_svd (PaperId) VALUES (?)", data_to_insert)
    cursor.execute("CREATE UNIQUE INDEX idx_paperid_selp ON selected_papers_svd(PaperId)")
    con.commit()


def load_data(con):
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
    fields_of_study = pd.read_sql("select * from fields_to_max_level", con=con)
    
    return papers_fields, fields_of_study


def main():
    
    model_url = MODEL_URL
    sqlite.register_adapter(np.int64, lambda val: int(val))
    con = sqlite.connect(database = db_file, isolation_level= None)


    prepare_tables(con)

    logging.info("loading valid papers")
    sample_papers(con) 
    
    
    logging.info("loading sampled papers and fields")
    papers_fields, fields_of_study = load_data(con) 
    
    field_to_index = {id: index for index, id in enumerate(fields_of_study['FieldOfStudyId'].unique())}
   
    logging.info("making sparse matrix and running SVD")
    papers_fields_sparse, _ = make_sparse(
        papers_fields, field_to_index, "PaperId", "FieldOfStudyId", "Score")
    
    svd, _  = run_svd(papers_fields_sparse, N_DIM)
    
   
    logging.info("saving model")

    if DRY_RUN:
        model_url += "_dry"
    
    with open(model_url + ".pkl", "wb") as f:
        dump(svd, f, protocol=5)
    
    logging.info("Done.")


if __name__ == "__main__":
    main()


