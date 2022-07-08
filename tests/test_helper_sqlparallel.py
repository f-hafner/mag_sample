#!/usr/bin/python
# -*- coding: utf-8 -*-

from src.dataprep.helpers.sqlparallel import SQLParallel
import pytest 
import os
import sqlite3 as sqlite
import pandas as pd
from pandas.testing import assert_frame_equal

table = "table2"
schema = "(id INT, value REAL)"
indexes = "create idx2 ON table2 (id) "

# define location for temporary files
@pytest.fixture
def filedir(temp_filedir):
    dir = temp_filedir / "tempfiles"
    return dir


@pytest.fixture
def sql_parallel(temp_db_file, filedir):
    A = SQLParallel(db_file = temp_db_file,
                    tbl = table, 
                    tbl_schema = schema,
                    filedir = filedir,
                    indexes = indexes)
    return A


def test__current_init_args(sql_parallel, temp_db_file, filedir):
    init_args = (f"{temp_db_file!r}, {table!r}, {schema!r}, "
                 f"{filedir!r}, 'chunk', 'all_collected.csv', "
                 f"{indexes!r}")
                
    assert sql_parallel._current_init_args() == init_args, \
             "wrong current init args"

    new_index = "newindex"
    sql_parallel.indexes = new_index
    new_init_args = (f"{temp_db_file!r}, {table!r}, {schema!r}, "
                     f"{filedir!r}, 'chunk', 'all_collected.csv', "
                     f"{new_index!r}")
    assert sql_parallel._current_init_args() == new_init_args, \
            "init args not updated"



def test_open_close(filedir, sql_parallel):
    assert not os.path.isdir(filedir), "filedir already exists"

    sql_parallel.open()
    assert os.path.isdir(filedir), "filedir does not exist"
    assert isinstance(sql_parallel.conn, sqlite.Connection), \
        "sqlite connection does not exist"

    sql_parallel.close()
    assert not os.path.isdir(filedir), "filedir is deleted"


def test_create_inputs(sql_parallel):
    sql_parallel.open()
    q = "select distinct id from table1"
    l = sql_parallel.create_inputs(sql = q, chunk_size = 2)
    output = [(0, [1, 2]), (1, [3, 4])]
    assert output == l, "correct inputs for iterating"


def test_db_dump(data1, sql_parallel):
    sql_parallel.open()
    # write to temp directory
    for i in data1["id"].unique():
        fn = sql_parallel.filedir / f"{sql_parallel.fn_chunks}-{i}.csv"
        data1.loc[data1["id"] == i, :].to_csv(fn, index = False)

    sql_parallel.db_dump()

    fn_full = sql_parallel.filedir / f"{sql_parallel.fn_full}"
    data_collected = pd.read_csv(fn_full, names = data1.columns) 
        # need file without header for sqlite .import
    #pdb.set_trace()
    assert_frame_equal(data1, data_collected)
    #assert data_collected.equals(data1), "correct data after combining files"

    with sql_parallel.conn as con:
        q = f"SELECT * FROM {sql_parallel.tbl}"
        data_in_db = pd.read_sql(sql = q, con = con)

    assert_frame_equal(data1, data_in_db)
    sql_parallel.close()



