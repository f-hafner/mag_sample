#!/usr/bin/python
# -*- coding: utf-8 -*-


# need: subprocess, etc! how to import them here?
import pandas as pd
from multiprocessing import Pool


# TODO: follow book pp. 99 for some further improvements
# TODO: give better names to class methods?
# TODO: review attributes -- which ones do I really need in the class? many could also just be passed on when
    # the function is executed? what are the "rules" here?
# TODO: allow here for limits? ie transform the query as for the parts?
# TODO: Q: does the child instance also open a new connection? why (not)? -- probably depends on what it inherits?
# In principle one can also run the deletion of the main table in the db in parallel to the 
    # processing of the separate files. 
    # tradeoffs: the longer it takes to delete the table (and indexes), relative to the mapreduce process, the more worth is it to run it in parallel.
    # https://stackoverflow.com/questions/19080792/run-separate-processes-in-parallel-python

from helpers.sqlparallel import SQLParallel
from helpers.sqlchunk import SQLChunk
# from helpers.functions import print_elapsed_time, analyze_db
from helpers.variables import db_file


n_cores = 5

def f(iteration_id, authors):
    # iterate 
    B = SQLChunk(A) # note: this takes as much time as using inputs[0][0] as a separate input for the function when save() is called
    q = "select AuthorId, YearLastPub, FirstName FROM author_sample WHERE AuthorId IN COND1"
    p = {"COND1": authors} 
    df = B.read_sql(query = q, params = p)
    # ... more calculations on df here ...  -> get df_out at the end
    df_out = df
    # save 
    B.write_part(df = df_out, iteration_id = iteration_id)


if __name__ == "__main__":
    # 1. Initiate 
    A = SQLParallel(db_file = db_file, tbl = "test", 
                    filedir = "flaviotest/", fn_parts = "parts",
                    fn_full = "all_collected", tbl_schema = "(AuthorId INT, YearLastPub INT, FirstName TEXT)",
                    indexes = ["create unique index idx_t_AuthorId ON test (AuthorId ASC)",
                                "create index idx_t_Year ON test (YearLastPub)"])

    A.open() # rename this? 
    inputs = A.create_inputs(sql = "select distinct authorid from author_sample limit 100", chunk_size = 10)

    # 2. Iterate
    with Pool(processes = n_cores) as pool:
        results = pool.starmap(f, inputs)
    print("queries finished")

    # 3. Save and clean
    A.db_dump()
    A.create_indexes()
    A.close()

    print("Done.")
    




## trying nested parallel
import concurrent.futures
import time

def f(x):
    return x*x

def slow():
    time.sleep(8)
    return 5

def fast():
    time.sleep(3)
    with Pool(processes = 3) as pool:
        print(pool.map(f, range(10)))
    return 10

def main():
    with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
        future1 = executor.submit(slow)
        future2 = executor.submit(fast)
        print(future1.result() + future2.result()) 
        print("Done.")

if __name__ == "__main__":
    main()

# is it obvious that these two things run in paralell? or does one what for another?
# see also here: https://stackoverflow.com/questions/63306875/combining-multithreading-and-multiprocessing-with-concurrent-futures
# and here https://stackoverflow.com/questions/49947935/nested-parallelism-in-python-multiprocessing