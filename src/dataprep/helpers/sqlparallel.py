#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3 as sqlite 
import subprocess
import os
import sys
from shutil import rmtree
import pandas as pd
import math

class SQLParallel: 
    """
    A class to use aggregate/window functions in sqlite with parallel in-memory operations.
    """
    def __init__(self, db_file, tbl, filedir, fn_parts, fn_full, tbl_schema):
        self.db_file = db_file
        # TODO: add option for read-only? how is best? make connection as a function with the argument . AND CHECK IF EXISTS!
            # note: when using all in one, there is no point in having a read-only connection b/c we will write to it later
            # perhaps add a separate method for initiating the db connection? think if this is worth!
        # TODO: how to add optional args? ie indexes?
        self.tbl = tbl 
        self.filedir = filedir
        self.fn_parts = fn_parts
        self.fn_full = fn_full
        self.tbl_schema = tbl_schema # example: "(AuthorId INTEGER, CoAuthorId INTEGER, Year INTEGER)" -- or other format? like json, jaml, dict, ...? 
    
    def __repr__(self): # TODO: how to make this easy with many args?
        return f"""connection {self.conn!r}, filedir {self.filedir}, filename parts {self.fn_parts}, filename full {self.fn_full}"""

    def db_dump(self):
        print("Combining files into one", flush = True)
        subprocess.run(f"tail -n +2 -q {self.filedir}/{self.fn_parts}-*.csv >> {self.fn_full}", shell = True)

        print("Dropping existing table and creating new empty one", flush = True)
        with self.conn as con:
            con.execute(f"DROP TABLE IF EXISTS {self.tbl}")
            con.execute(f"CREATE TABLE {self.tbl} {self.tbl_schema}")

        print("Reading file into db", flush = True) # TODO: set flush as a global setting in the class?
        subprocess.run(
            ["sqlite3", self.db_file,
            ".mode csv",
            f".import {self.fn_full} {self.tbl}"]
        )

    def create_indexes(self):
        # TODO: check that they do not exist already?
        # TODO: make it amenable to multiple indexes
        self.conn.execute(self.index)

    def close(self):
        # TODO: add analyze_db -- internal fct. o
        print("Closing sqlite connection", flush = True)
        self.conn.close()
        print("Removing temporay files and directories")
        rmtree(self.filedir)

    def open(self): 
        print("Opening sqlite connection...", flush = True)
        self.conn = sqlite.connect(database = self.db_file, isolation_level= None)  # Q: how to make this inherit from sqlite?
        print(f"Generating temporary file directory {self.filedir}", flush = True)
        if os.path.isdir(self.filedir):
            sys.exit("You specified an existing directory.") #  TODO: raise exeptions here instead -- directory already exists 
        os.mkdir(self.filedir)

        # TODO: add other stuff?
        # TODO: delete the self.conn attribute when closed? or how to show that it is closed?
        # TODO: does it matter (for speed?) whether the connection is open or not during the reading of the children?

    def make_iterator(self, sql, n_groups, chunk_size): # TODO: this would be an alternative to get the columns -- which one is better to the below?
            # TODO: should these things be set up when the class is initiated? not necessarily.
        "Make an iterator from a sql query for a single query over which to iterate "
        with self.conn as con:
            col_list = con.execute(sql).fetchall()
        
        col_list = [i[0] for i in col_list]
        n = len(col_list)

        n_groups = math.ceil(n / chunk_size)
        it = (i, col_list[range(i * chunk_size), min(i * chunk_size + chunk_size, n)].tolist() for i in range(n_groups))

        return(it)


   
    
