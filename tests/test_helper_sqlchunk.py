#!/usr/bin/python
# -*- coding: utf-8 -*-

from src.dataprep.helpers.sqlchunk import SQLChunk
import pytest 
import os
import pandas as pd
from pandas.testing import assert_frame_equal


@pytest.fixture
def sql_chunk(sql_parallel):
    B = SQLChunk(sql_parallel)
    return B


def test_sqlchunk(temp_db_file, parallel_params, sql_chunk, sql_parallel):
    params = parallel_params
    init_args = (f"{temp_db_file!r}, {params['table']!r}, {params['schema']!r}, "
                 f"{params['filedir']!r}, 'chunk', 'all_collected.csv', "
                 f"{params['indexes']!r}")
    init_args = f"SQLParallel({init_args})"
    assert sql_chunk._current_init_args() == init_args, "not instantiated correctly"

    new_index = "newindex"
    sql_parallel.indexes = new_index
    new_init_args = (f"{temp_db_file!r}, {params['table']!r}, {params['schema']!r}, "
                     f"{params['filedir']!r}, 'chunk', 'all_collected.csv', "
                     f"{new_index!r}")
    new_init_args = f"SQLParallel({new_init_args})"
    assert sql_chunk._current_init_args() == new_init_args, \
            "init args not updated"


def test_read_sql(sql_chunk, data1):
    keep_ids = [1, 2]
    q = "SELECT * from table1 WHERE id IN cond1"
    c = {"cond1": keep_ids}
    df = sql_chunk.read_sql(query = q, params = c)
    expected_df = data1.loc[data1["id"].isin(keep_ids), :].copy()
    assert_frame_equal(df, expected_df)
                

def test_write_chunk(sql_chunk, data1, parallel_params, sql_parallel):
    params = parallel_params
    iter_id = "1"
    sql_parallel.open()
    sql_chunk.write_chunk(df = data1, iteration_id = iter_id)
    assert os.path.exists(f"{params['filedir']}")
    fn_expected = f"{params['filedir']}/{sql_chunk.fn_chunks}-{iter_id}.csv"
    assert os.path.exists(fn_expected)
    df_read = pd.read_csv(fn_expected)
    assert_frame_equal(df_read, data1)
    sql_parallel.close()
