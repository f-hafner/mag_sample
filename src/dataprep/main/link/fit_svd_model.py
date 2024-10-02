"""Prepare data and fit truncated SVD on subsample of papers"""


# import ibis
# from ibis import _
import matplotlib.pyplot as plt  
import numpy as np 
import sqlite3 as sqlite
import numpy as np      
import pandas as pd 
from pickle import dump

from scipy.sparse import csr_matrix                                                                                                                                                         
from sklearn.decomposition import TruncatedSVD  
import scipy
from helpers.variables import db_file, insert_questionmark_doctypes, keep_doctypes


random_seed = 583523592352
sample_size = 1_000_000
N_DIM = 1024

# TODO: is this correct? -- doublecheck with Christoph
DRY_RUN = True
SAMPLE_SIZE = 1_000_000
max_level = 2
start_year = 1980
end_year = 2020

MODEL_URL = "/mnt/ssd/AcademicGraph/svd_model"


def make_sparse(long_df, field_to_index, rows="AffiliationId", cols="FieldOfStudyId", value_col="score"): 
    # col = concepts_affiliations['AffiliationId']
    # row = 
    """Create sparse matrix from a dataframe.

    Args:
        col (np.ndarray): values for index 0 in the resulting sparse matrix
        row (np.ndarray): values for index 1 in the resulting sparse matrix.
        row_map: mapping of all possible row values to identifiers.
        data: non-zero values to to fill the sparse matrix.  

    Returns:
    (sparse matrix, map from row IDs in dataframe to row index in the sparse matrix)
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
    (svd model, embeddings in subspace)
    """
    svd = TruncatedSVD(n_components=n_components, random_state=42) 
    embs = svd.fit_transform(in_matrix)
    
    print(f"Original matrix shape: {in_matrix.shape}")
    print(f"Reduced matrix shape: {embs.shape}")
    print(f"Explained variance ratio: {svd.explained_variance_ratio_.sum():.4f}") 
    
    return svd, embs



generator = np.random.default_rng(random_seed)
sqlite.register_adapter(np.int64, lambda val: int(val))
con = sqlite.connect(database = db_file, isolation_level= None)




print("making fields_to_max_level", flush=True)
con.execute(
    """CREATE TEMP TABLE fields_to_max_level AS
    SELECT FieldOfStudyId
    FROM FieldsOfStudy
    WHERE Level > 0 AND Level <= (?)
    """
    , (max_level,)
)
con.execute("CREATE UNIQUE INDEX idx_temp1 ON fields_to_max_level(FieldOfStudyId ASC)")

print("creating valid papers", flush=True)

sql_valid_papers = f"""CREATE TEMP TABLE valid_papers AS
    SELECT PaperId
    FROM Papers
    WHERE Year >= (?)
    AND Year <= (?)
    AND DocType IN ({insert_questionmark_doctypes})"""
if DRY_RUN:
    sql_valid_papers += " LIMIT 1000"

print(sql_valid_papers)

con.execute(
    sql_valid_papers, (start_year, end_year, *keep_doctypes)
)


print("loading valid papers", flush=True)
all_papers = pd.read_sql("SELECT * FROM valid_papers", con=con) 


if DRY_RUN:
    subsample = all_papers.sample(n=500, random_state=generator)
else:
    subsample = all_papers.sample(n=SAMPLE_SIZE, random_state=generator)

print("writing sampled papers", flush=True)

cursor = con.cursor()
data_to_insert = [(x,) for x in subsample["PaperId"].values]
# sql_delete = "DROP TABLE IF EXISTS selected_papers_svd"
sql_create = "CREATE TEMP TABLE selected_papers_svd (PaperId INT)"

cursor.execute(sql_create)
cursor.executemany("INSERT INTO selected_papers_svd (PaperId) VALUES (?)", data_to_insert)
cursor.execute("CREATE UNIQUE INDEX idx_paperid_selp ON selected_papers_svd(PaperId)")
con.commit()


print("loading sampled papers and fields", flush=True)
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

field_to_index = {id: index for index, id in enumerate(fields_of_study['FieldOfStudyId'].unique())}
field_to_index_map = np.vectorize(field_to_index.get)


papers_fields_sparse, row_to_index = make_sparse(
    papers_fields, field_to_index, "PaperId", "FieldOfStudyId", "Score")

svd, embs = run_svd(papers_fields_sparse, N_DIM)


if DRY_RUN:
    MODEL_URL += "_dry"

with open(MODEL_URL + ".pkl", "wb") as f:
    dump(svd, f, protocol=5)

print("Done.")



