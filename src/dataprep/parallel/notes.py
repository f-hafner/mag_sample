

# example 1: unique author-coauthor by year 
###############################
f"""SELECT a.AuthorId, d.AuthorId AS CoAuthorId, d.Year 
    FROM PaperAuthorUnique AS a
    INNER JOIN (
        SELECT b.*, c.Year 
        FROM PaperAuthorUnique AS b
        INNER JOIN (
            SELECT PaperId, Year 
            FROM Papers
            WHERE DocType IN ({insert_questionmark_doctypes})
        ) AS c
        USING (PaperId)
    ) AS d
    ON (a.PaperId = d.PaperId and a.AuthorId != d.AuthorId)
    -- drop authors not in author_sample
    INNER JOIN (
        SELECT AuthorId
        FROM author_sample
        WHERE AuthorId IN ({qmark_current_ids})
    ) AS e ON (a.AuthorId = e.AuthorId)
    INNER JOIN (
        SELECT AuthorId
        FROM author_sample
    ) AS f on (CoAuthorId = f.AuthorId)
"""
# then apply pd.drop_duplicates


# example 2: paperauthorunique
###############################
f"""    
SELECT a.PaperId, a.AuthorId 
FROM PaperAuthorAffiliations a
INNER JOIN (
    SELECT PaperId 
    FROM PaperMainFieldsOfStudy 
) USING (PaperId)
WHERE a.PaperId IN ({qmark_current_ids})
"""
    # then apply pd.drop_duplicates
    # iterating over paperids


# example 3: author_sample
###############################

# query 1: names
firstnames = f"""select AuthorId, SUBSTR(TRIM(NormalizedName),1,instr(trim(NormalizedName)||' ',' ')-1) AS FirstName
    FROM Authors 
    WHERE AuthorId IN ({qmark_current_ids})
"""

# query 2: authors and papers 
table2 = f"""
SELECT a.AuthorId, b.Year
FROM PaperAuthorUnique 
INNER JOIN (
    SELECT Paperid, DocType, Year
    FROM Papers
)
WHERE b.DocType IN ({insert_questionmark_doctypes})
    AND AuthorId IN ({qmark_current_ids})
"""

# then apply pandas on table2: max(year), min(year), number of papers
    # by AuthorId
    # then keep authors if PaperCount >= 2 and PaperCount / (YearLastPub - YearFirstPub) <= 20 
# join table1
# iterating over authorids

# question:
    # best way to store this information?
    # use classes/objects?


class Account:
    def __init__(self, owner, balance):
        self.owner = owner
        self.balance = balance 
    
    def deposit(self, amount):
        self.balance += amount


# idea:
    # the object is fixed with queries, pd dfs and pd functions
    # each iteration creates a new object with given ids
    # 


# sketch 

# need: subprocess, etc! how to import them here?
import subprocess
import sqlite3 as sqlite
import pandas
from multiprocessing import Pool


# TODO:
    # question: define all methods within the class? or define methods separately and require them 
        # to work on a particular class? how do I do this?

class BigClass: # TODO: give a good name
    """A class to parallelize aggregation/window functions in sqlite"""
    def __init__(self, db_file, table, filedir, filename_parts, filename_full, schema):
        self.db_file = db_file
        self.conn = sqlite.connect(database = db_file, isolation_level= None)  # Q: how to make this inherit from sqlite? 
        # TODO: add option for read-only? how is best? make connection as a function with the argument . AND CHECK IF EXISTS!
        # TODO: how to add optional args? ie indexes?
        self.tbl = table
        self.filedir = filedir
        self.fn_parts = filename_parts
        self.fn_full = filename_full
        self.schema = schema # example: "(AuthorId INTEGER, CoAuthorId INTEGER, Year INTEGER)" -- or other format? like json, jaml, dict, ...? 
    
    # todo: make temp directory
        # how?

    def __repr__(self): # TODO: how to make this easy with many args?
        return f"""connection {self.conn!r}, filedir {self.filedir}, filename parts {self.fn_parts}, filename full {self.fn_full}"""

    def db_dump(self):
        print("Combining files into one", flush = True)
        subprocess.run(f"tail -n +2 -q {self.filedir}/{self.fn_parts}-*.csv >> {self.fn_full}", shell = True)

        print("Dropping existing table and creating new empty one", flush = True)
        self.conn.execute(f"DROP TABLE IF EXISTS {self.tbl}")
        self.conn.execute(f"CREATE TABLE {self.tbl} {self.schema}")

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
        self.conn.close()
        # TODO: add remove temp directory
        # TODO: add other stuff?

    def write_part(self):
        iteration_id = 2 # TODO: define where??
        self.dataframe.to_csv(f"{self.filedir}/{self.fn_parts}-{iteration_id}.csv")


# sample usage 
###########################
# todo: create a subclass (?) with the additional data:
    # 

from parallel import BigClass, SubClass

def myfunction(iteration_id):
    b = SubClass(a) # TODO: define some subclass. how to pass a? as initarg?
    df1 = b.query("select * from authors limit 10")
    df2 = b.query("select * from author_sample limit 10")
    d = df1.join(df2, by = "id")
    # add this d to the class? how? ie an element for whom methods are defined but which is not necessary at creation?
    a.write_part(iter_id = iteration_id) # or pass iteration_id above 
    # TODO: either initialize the subclass above and add the write_part to it
        # or better, make a single function, and then use the attributes of the bigclass as arguments?


if __name__ == "__main__":
    a = BigClass(...)
    # TODO: time! capture start time at initialization, calculate full time at end; print(?)
    list_in = ... # define some iterable 

    with Pool() as pool:
        results = pool.starmap(myfunction, list_in)
    print("--queries finished.")

    a.db_dump()
    a.create_index()
    a.close()


############# try it out with some simple example

class MyClass:
    def __init__(self, db_file, filename_part):
        self.db_file = db_file
        self.fn_part = filename_part
    def db_dump(self):
        print(f"using {self.db_file} to make {self.fn_part}.")
    def write_part(self, id):
        print(f"id is {id}.") 
    def write_df(self):
        # TODO: handling attribute errors?
        print(f"I have a dataframe {self.dataframe}.")

a = MyClass("myfile.sqlite", "part")

a.db_dump()
a.write_part(id = 3)
# add a df
a.dataframe = "dataframe"