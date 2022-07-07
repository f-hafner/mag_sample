#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
SQLParallel
----------

A container for data and methods to for 
parallel in-memory data processing from a sqlite database 
with pandas.
"""

import sqlite3 as sqlite 
import subprocess
import os
import sys
from shutil import rmtree
import pandas as pd
import math
import pdb
import inspect

class SQLParallel: 
    """
    A class to use aggregate/window functions in sqlite with parallel 
        in-memory operations.

    Parameters
    ---------
    db_file : String with the relative or absolute path to the database. 

    tbl : A string; the name of the table to be created on the database.

    tbl_schema: The schema of `tbl` as a SQLite string.

    filedir : String with the path to the temporary file directory where  
        temporary files are stored during processing.

    fn_chunks : Name of the temporary files to be created for each chunk.

    fn_full : Name of the temporary file that is collected from the single chunks.

    indexes : A list of strings with Sqlite commands for creating indexes.

    Example
    --------
    >>> A = SQLParallel(
            db_file = "path/to/database.sqlite", tbl = "mytable", 
            filedir = "tempdir/", fn_chunks = "chunk",
            fn_full = "all_collected", 
            tbl_schema = "(AuthorId INT, YearLastPub INT, FirstName TEXT)",
            indexes = ["create unique index idx_t_AuthorId ON mytable (AuthorId ASC)",
                       "create index idx_t_Year ON mytable (YearLastPub)"]
            )
    
    """
    def __init__(self, db_file, tbl, tbl_schema, filedir, fn_chunks = "chunk", 
                 fn_full = "all_collected", indexes = None):
        self.db_file = db_file
        # TODO: add option for read-only? how is best? make connection as a function with the argument . AND CHECK IF EXISTS!
            # note: when using all in one, there is no point in having a read-only connection b/c we will write to it later
            # perhaps add a separate method for initiating the db connection? think if this is worth!
        self.tbl = tbl 
        self.tbl_schema = tbl_schema  
        self.filedir = filedir
        self.fn_chunks = fn_chunks
        self.fn_full = fn_full
        self.indexes = indexes
    
    def __repr__(self):
        args = self._current_init_args()
        return f"{self.__class__.__name__}({args})"
    
    def _current_init_args(self):
        """
        Get the current arguments for __repr__.

        Note
        ----
        For child classes that inherit arguments from SQLParallel *instances*,
            the following is important:
            1. The parent needs to be assigned to self._parent. 
            2. The __init__ call needs to have the parameter `parent` that refers to 
                to the parent object. 
        """
        init_params = inspect.signature(self.__init__).parameters
        all_args = vars(self)
        # pdb.set_trace()
        # return only the parameters used at instantiation
            # if we later add an arbitrary arguments, we could
            # not use it to re-create the object later if there are no **kwargs
        init_args = [all_args[k] for k in init_params.keys() if k != "parent"] 
        init_args = ", ".join([f"{i!r}" for i in init_args])
        if "_parent" in all_args.keys():
            parent_arg = all_args["_parent"]
            if len(init_args) > 0:
                init_args = f"{parent_arg!r}, {init_args}"
            else:
                init_args = f"{parent_arg!r}"
        return init_args

    
    def db_dump(self):
        """
        Dump the temporary files into the database. 
        """

        print("Combining files into one", flush = True)
        chunks = f"{self.filedir}/{self.fn_chunks}"
        full = f"{self.filedir}/{self.fn_full}"
        subprocess.run(f"tail -n +2 -q {chunks}-*.csv >> {full}", shell = True)

        print("Dropping existing table and creating new empty one", flush = True)
        with self.conn as con:
            con.execute(f"DROP TABLE IF EXISTS {self.tbl}")
            con.execute(f"CREATE TABLE {self.tbl} {self.tbl_schema}")

        print("Reading file into db", flush = True) # TODO: set flush as a global setting in the class?
        subprocess.run(
            ["sqlite3", self.db_file,
            ".mode csv",
            f".import {self.filedir}/{self.fn_full} {self.tbl}"]
        )

    def create_indexes(self):
        """
        Create indexes on the table `tbl`. 
        """
        print("Creating indexes", flush = True)
        if self.indexes is not None:
            with self.conn as write_con:
                for i in self.indexes:
                    # print(i)
                    write_con.execute(i)
            # TODO: check that they do not exist already? does it throw an exception or not if it does not work?

    def close(self):
        """Close the SQLParallel object."""
        # TODO: add analyze_db -- internal fct. o
        self._analyze_db()
        print("Closing sqlite connection", flush = True)
        self.conn.close()
        print("Removing temporay files and directories")
        rmtree(self.filedir)


    def _analyze_db(self):
        """
        Run `analyze` commands on `con` according to sqlite recommendations.
        """
        print("Running ANALYZE... \n", flush = True)
        with self.conn.cursor() as cur:
            cur.execute("PRAGMA analysis_limit = 1000")
            cur.execute("PRAGMA optimize")
        

    def open(self): 
        """Open the SQLParallel object"""
        print("Opening sqlite connection...", flush = True)
        self.conn = sqlite.connect(database = self.db_file, isolation_level = None)  # Q: how to make this inherit from sqlite?
        print(f"Generating temporary file directory {self.filedir}", flush = True)
        if os.path.isdir(self.filedir):
            sys.exit("You specified an existing directory.") #  TODO: raise exeptions here instead -- directory already exists 
        os.mkdir(self.filedir)

        # TODO: add other stuff?
        # TODO: delete the self.conn attribute when closed? or how to show that it is closed?
        # TODO: does it matter (for speed?) whether the connection is open or not during the reading of the children?

    def create_inputs(self, sql, chunk_size): 
        """
        Make an iterator from a sql query for a single query 
            over which to iterate. Th

        Parameters
        ----------
        sql : SQLite query to extract the unique entries from a 
            column in a table. 

        chunk_size : Number of units to process in one chunk in 
            the parallel routine.

        Returns
        -------
        A list of tuples: For each tuple, the first entry is the 
            chunk identifier; the second entry is the units
            to be processed.

        Example
        -------
        >>> ## create inputs
        >>> query = "select distinct authorid from author_sample limit 100"
        >>> inputs = A.create_inputs(sql = query, chunk_size = 10)
        >>> f = some_function(iteration_id, units)

        >>> # can use for iterating:
        >>> with Pool(processes = n_cores) as pool:
                results = pool.starmap(f, inputs)
        """

        with self.conn as con:
            col_list = con.execute(sql).fetchall()
        
        col_list = [i[0] for i in col_list]
        n = len(col_list)

        n_groups = math.ceil(n / chunk_size)
        it = [(i, col_list[i * chunk_size:(min(i*chunk_size + chunk_size, n))]) 
                for i in range(n_groups)]

        return(it)


   
    
