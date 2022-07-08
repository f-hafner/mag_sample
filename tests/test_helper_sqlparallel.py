#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sqlite3 as sqlite
import pandas as pd
from pandas.testing import assert_frame_equal

def test__current_init_args(sql_parallel, temp_db_file, parallel_params):
    params = parallel_params
    init_args = (f"{temp_db_file!r}, {params['table']!r}, {params['schema']!r}, "
                 f"{params['filedir']!r}, 'chunk', 'all_collected.csv', "
                 f"{params['indexes']!r}")
                
    assert sql_parallel._current_init_args() == init_args, \
             "wrong current init args"

    new_index = "newindex"
    sql_parallel.indexes = new_index
    new_init_args = (f"{temp_db_file!r}, {params['table']!r}, {params['schema']!r}, "
                     f"{params['filedir']!r}, 'chunk', 'all_collected.csv', "
                     f"{new_index!r}")
    assert sql_parallel._current_init_args() == new_init_args, \
            "init args not updated"



def test_open_close(parallel_params, sql_parallel):
    dir = parallel_params["filedir"]
    assert not os.path.isdir(dir), "filedir already exists"

    sql_parallel.open()
    assert os.path.isdir(dir), "filedir does not exist"
    assert isinstance(sql_parallel.conn, sqlite.Connection), \
        "sqlite connection does not exist"

    sql_parallel.close()
    assert not os.path.isdir(dir), "filedir is deleted"


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



