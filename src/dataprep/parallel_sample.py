#!/usr/bin/python
# -*- coding: utf-8 -*-


# need: subprocess, etc! how to import them here?
import pandas
from multiprocessing import Pool


from helpers.sqlparallel import SQLParallel
from helpers.sqlchunk import SQLChunk
# from helpers.functions import print_elapsed_time, analyze_db
from helpers.variables import db_file

A = SQLParallel(db_file = db_file, tbl = "test", 
                filedir = "flaviotest/", fn_parts = "parts",
                fn_full = "all_collected", tbl_schema = "")


A.open() # rename this? 

q = "select distinct authorid from author_sample limit cond1"
p = {"cond1": (30, )}
authors = A.read_sql(query = q, params = p)

A.get_column("select distinct authorid from author_sample limit 10")

# can initiate chunk size here, and then directly generate the iterable?
    # ie, transform the code below into a function in SQLParallel. store it in an attribute(?) of the class or just return it as an object?
        # and then use it as input for starmap?

# TODO: continue with make_iterator: make it work like in the prep_collab function


# l = A.conn.execute("select distinct paperid from papers limit 10").fetchall()
# l = [i[0] for i in l]


B = SQLChunk(A)

q = "select PaperId, PaperTitle from Papers where PaperId in cond1 "
p = {"cond1": (2789336, 9552966, ),
          "cond2": ("Journal",)} 
B.read_sql(query = q, params = p)

A.close()



#quit()


# how to generate the things to iterate over?
    # do we even need a user-facing function to open the sql connection in SQLParallel?
        # could instead initiate an iterator as an attribute of the class? and then use it as input in the apply function?
    # but we'd need a write connection at the end? create it there? 


# does the child instance also open a new connection? why (not)?



if __name__ == "__main__":
    a = SQLParallel(...)
    # TODO: time! capture start time at initialization, calculate full time at end; print(?)
    list_in = ... # define some iterable 

    with Pool() as pool:
        results = pool.starmap(myfunction, list_in)
    print("--queries finished.")

    a.db_dump()
    a.create_index()
    a.close()


vars(a)
vars(b)

# a.db_dump()
# a.write_part(id = 3)
# b.write_part_new()
# b.query("select x from y where x in <<filter_ids>>")
# b.query("wrong string")
# # add a df
# a.dataframe = "dataframe"



