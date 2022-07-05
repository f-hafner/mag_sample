"""
SQLChunk
----------

A subclass of SQLParallel with some 
methods for loading and writing tables.
"""


# what does the subclass need to be able to do?
    # loading +/- complex queries from the database with the current ids
    # generate the questionmarks, insert them at the right place
    # write a dataframe to a file in the specified directory 

import pandas as pd
import sqlite3 as sqlite
from .sqlparallel import SQLParallel
import pdb

_marker = object()


class SQLChunk(SQLParallel):
    """
    A subclass of SQLParallel that dynamically inherits attributes from the parent.
        Use to read tables from a database and save processed files.

    Example:
    --------
    >>> # A = SQLParallel(...)
    >>> B = SQLChunk(A) 
    >>> q = "SELECT AuthorId, YearLastPub, FirstName 
             FROM author_sample WHERE AuthorId IN COND1"
    >>> p = {"COND1": [1, 2, 3]} 
    >>> df = B.read_sql(query = q, params = p)
    >>> # ... more calculations on df here ...  -> get df_out at the end
    >>> df_out = df
    >>> # save 
    >>> B.write_chunk(df = df_out, iteration_id = iteration_id)
    """

    _inherited = ["db_file", "fn_chunks", "filedir"]
        # TODO: if these children could open/close their own connections -- this may further speed up? since one read does not have to wait for another?
            # check this

    def __init__(self, parent):
        self._parent = parent

    def __getattr__(self, name, default=_marker):
        if name in self._inherited:
            try:
                return getattr(self._parent, name)
            except AttributeError:
                if default is _marker:
                    raise
                return default
        if name not in self.__dict__:
            raise AttributeError(name)
        return self.__dict__[name]

    def write_chunk(self, df, iteration_id):
        """
        Write a chunk of processed data to csv.

        Parameters
        ----------
        df : A pandas dataframe.

        iteration_id : The iteration id number that uniquely
            identifies the chunk.

        """
        chunks = f"{self.filedir}/{self.fn_chunks}"
        df.to_csv(f"{chunks}-{iteration_id}.csv", index = False)

    
    def read_sql(self, query, params): 
        """
        Query the database and load into a pandas DataFrame.

        Parameters
        ----------
        query : A SQLite query from the database. Conditions 
            have to be marked by a unique string, which is
            referred to in `params`.

        params : A dictionary. The keys are the same strings
            used in query. The values is a list of values
            that are to be kept.

        Example
        -------
        >>> B = SQLChunk(A) 
        >>> q = "select AuthorId, YearLastPub, FirstName FROM author_sample WHERE AuthorId IN COND1"
        >>> p = {"COND1": [1, 2, 3]} 
        >>> df = B.read_sql(query = q, params = p)

        """
        # TODO: it should also accept further optional argument to pass to pd.read_sql
        query_dict = self._prep_query(query = query, params = params)

        # print(query) # TODO: handle exceptions eg if the query does not work would sqlalchemy work better here? 
        read_conn = sqlite.connect(database = f"file:{self.db_file}?mode=ro", 
                                   isolation_level= None, uri = True)
        with read_conn:
            df = pd.read_sql(sql = query_dict["query"], con = read_conn, params = query_dict["parameters"])

        read_conn.close()
        return(df)


    def _prep_query(self, query, params): # TODO: this could go out of the class I guess? but where?
        """
        Prepare a SQLite query from a statement and a dictionary 
            with conditions.

        Parameters
        ----------
        query : A SQLite query from the database. Conditions 
            have to be marked by a unique string, which is
            referred to in `params`.

        params : A dictionary. The keys are the same strings
            used in query. The values is a list of values
            that are to be kept.
        """
        value_dict = {k: ",".join(["?" for i in range(len(v))]) for k, v in params.items() }
        position_dict = {k: query.find(k) for k in params.keys()}
        # TODO: check that each string in dict only occurs once -- otherwise impossible

        for k in value_dict.keys():
            query = query.replace(k, f"({value_dict[k]})")
            # note: still need to know the order for the input to sql query!
        
        value_order = sorted(position_dict)
        arguments = [list(params[k]) for k in value_order]
        arguments = sum(arguments, [])
        out = {"query": query, "parameters": arguments}
        return(out)








