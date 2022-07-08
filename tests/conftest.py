
import pytest
import os
import sqlite3 as sqlite
from src.dataprep.helpers.sqlparallel import SQLParallel


import numpy as np 
import pandas as pd 

# Data
#------------
@pytest.fixture
def data1():
    "Generate some data for database"
    id = np.repeat([1,2,3,4], 5)
    value = np.random.rand(id.shape[0])
    df = pd.DataFrame({"id": id, "value": value})
    return df


# Files and directories
#------------
@pytest.fixture
def temp_db_file(tmp_path, data1):
    "Initiate db file and dump some test data."
    db_file = tmp_path / "tempdb.sqlite"
    open(db_file, "w").close()
    assert os.path.exists(db_file)
    con = sqlite.connect(db_file)
    with con:
        data1.to_sql(con = con, name = "table1", index = False)
        con.execute("CREATE INDEX idx1_id ON table1(id)")
    
    con.close()
    return db_file

# This directory exists at the beginning of the tests 
    # we use it below to create new directories in it
@pytest.fixture
def mock_filedir(tmp_path_factory):
    "directory in which to create a directory for SQLParallel."
    dir = tmp_path_factory.mktemp("tempdata")
    return dir

# use this dict to instantiate classes and check their setup
    #Note: filedir here is to be created as temporary directory
        # for the class
@pytest.fixture
def parallel_params(mock_filedir):
    out = {"table": "table2",
           "schema": "(id INT, value REAL)",
           "indexes": "create idx2 ON table2 (id)",
           "filedir": mock_filedir / "parallelfiles" 
           }
    return(out)


# Class instances
#------------
@pytest.fixture
def sql_parallel(temp_db_file, parallel_params):
    A = SQLParallel(db_file = temp_db_file,
                    tbl = parallel_params["table"], 
                    tbl_schema = parallel_params["schema"],
                    filedir = parallel_params["filedir"],
                    indexes = parallel_params["indexes"])
    return A




