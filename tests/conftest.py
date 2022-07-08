
import pytest
import os
import sqlite3 as sqlite

import numpy as np 
import pandas as pd 


# generate some data 

@pytest.fixture
def data1():
    id = np.repeat([1,2,3,4], 5)
    value = np.random.rand(id.shape[0])
    df = pd.DataFrame({"id": id, "value": value})
    return df


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

@pytest.fixture
def temp_filedir(tmp_path_factory):
    dir = tmp_path_factory.mktemp("tempdata")
    return dir


