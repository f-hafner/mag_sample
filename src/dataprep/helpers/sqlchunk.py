
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
    Can read tables from a database and save processed files.
    """

    _inherited = ["db_file", "fn_part", "filedir"]
        # TODO: if these children could open/close their own connections -- this may further speed up? since one read does not have to wait for another?
            # check this

    def __init__(self, parent):
        self._parent = parent
        self.iteration_id = "specific to me" # TODO: to be defined

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

    def write_part(self):
        iteration_id = 2 # TODO: define where??
        self.dataframe.to_csv(f"{self.filedir}/{self.fn_parts}-{iteration_id}.csv")

    
    def read_sql(self, query, params): 
        """
        query the database with a read-only connection and return a pd dataframe of the resulting table. 
        see the arguments for prep_query
        """
        # TODO: it should also accept further optional argument to pass to pd.read_sql
        query_dict = self.prep_query(query = query, params = params)

        # print(query) # TODO: handle exceptions eg if the query does not work would sqlalchemy work better here? 
        read_conn = sqlite.connect(database = f"file:{self.db_file}?mode=ro", 
                                   isolation_level= None, uri = True)
        with read_conn:
            df = pd.read_sql(sql = query_dict["query"], con = read_conn, params = query_dict["parameters"])

        read_conn.close()
        return(df)


    def prep_query(self, query, params): # TODO: this could go out of the class I guess? but where?
        """
        Prepare a sqlite query from a statement and a dictionary with conditions
        query: string of sqlite queries. params: a dict mapping strings in query to values to be conditioned on
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





   
    
    # TODO: add method for querying (including qmarks), method for generating question marks given an input, move the write_part method to here(?), 

