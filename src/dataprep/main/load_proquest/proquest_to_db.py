#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Read in proquest, clean and load to database
"""

# Note: numeric_date is the publiation date of the dissertation, while grad_year is the 
    # graduation year of the person. In some cases, these lie far apart, and it could be an error 
    # To find the errors, we'd have to compare the original folders in proquest (the year range) with the 
        # graduation year/publication year, and assign the one relevant year
    # But a simpler and probably safer solution is to just drop observations where the years are too far apart from each other

# this is a utility for group and counting occurences by group
#dt = authors.groupby("university_location").size().reset_index(name = "counts").sort_values(by = "counts", ascending = False)

# TODO: extract country / tidy university names? 

import subprocess
import sqlite3 as sqlite
import argparse
import os
import pandas as pd
import numpy as np 

from helpers.variables import db_file, datapath, databasepath
from helpers.functions import analyze_db, normalize_string, drop_firstword

path_proquest = "extract_november122021/"
con = sqlite.connect(database = db_file, isolation_level= None)


# ## Read files
print("Reading files... \n")
advisors = pd.read_csv(datapath + path_proquest + "advisors.csv", 
                        sep = "\t", 
                        usecols = ["goid", "position", "lastname", "firstname"])
authors = pd.read_csv(datapath + path_proquest + "authors.csv", 
                        sep = "\t", 
                        usecols = ["goid", "position", "lastname", "firstname", "middlename", "degree_year", "degree_level", "university_location",
                                    "degree_description", "university", "numeric_date", "department", "doctitle"])
fields = pd.read_csv(datapath + path_proquest + "fields.csv", 
                        sep = "\t", 
                        usecols = ["goid", "position", "fieldcode", "fieldname"],
                        dtype = {"fieldcode": "str"})
authors = authors.rename(columns = {"doctitle": "originaltitle"})

# ## Clean 
print("Cleaning... \n")

# ### Keep only PhD degrees

# define PhD degrees
phd_degrees = ["Ph.D.", "Dr.", "D.Phil"]
phd_descriptions = "PhD|Ph.D.|Doctor|Docteur|Doktor"
# Note: this will keep also "Doctor of Education"/"Doctor of Engineering" etc, but the distinction
    # to PhD is not so clear (at least for these two, one can also go to research after graduation)
authors = authors.loc[(authors["degree_level"].isin(phd_degrees)) | 
                        (authors["degree_description"].str.contains(phd_descriptions, regex = True))
                    ]
n_authors = authors.shape[0]
print(f"Starting with {n_authors} authors with PhD. \n")

# ### Drop authors with missing lastname 
authors = authors.loc[~(authors["lastname"].isna() | authors["firstname"].isna())]
print(f"Dropped {n_authors - authors.shape[0]} authors with missing first or last name. \n")
n_authors = authors.shape[0]

# ### Normalize names, fields and titles
for name in ["lastname", "firstname", "middlename"]:
    authors[name] = normalize_string(authors[name])
    if name != "middlename":
        advisors[name] = normalize_string(advisors[name])

fields["fieldname"] = normalize_string(fields["fieldname"], replace_hyphen = " ")
authors["uni_normalized"] = normalize_string(authors["university"], replace_hyphen = " ")
authors["uni_normalized"] = authors["uni_normalized"].replace("the university", "university", regex = True)
authors["uni_normalized"] = authors["uni_normalized"].str.strip()

# ### Normalize titles
authors["thesistitle"] = normalize_string(authors["originaltitle"], replace_hyphen = " ") # also seems analoguous to MAG

# ### Drop records with multiple authors, degree_year > 1900, missing university 
authors["max_position"] = (authors["position"].
                            groupby(authors["goid"]).
                            transform("max"))
authors = (authors
            .loc[authors["max_position"] == 0]
            .drop(columns = ["max_position"]))
authors = authors.loc[authors["degree_year"] > 1900]
authors = authors.loc[~authors["uni_normalized"].isna()]

print(f"Dropped {n_authors - authors.shape[0]}  authors whose thesis has multiple authors, or degree year < 1900, or missing university. \n")
n_authors = authors.shape[0]

# ### Further normalize uni locations
    # Remove country (in location) from uni_normalized; drop "the" when at start of name
authors["location_normalized"] =  normalize_string(authors["university_location"], replace_hyphen="")
authors["uni_normalized"] = authors.apply(lambda row: row.uni_normalized.replace(row.location_normalized, ""), axis = "columns")
authors["uni_normalized"] = authors["uni_normalized"].apply(lambda s: drop_firstword(s, "the"))
authors["uni_normalized"] = authors["uni_normalized"].str.replace("united kingdom", "") 
    # NOTE: some other universities are also wrong there (germany, poland, ...), but UK is the most important for our purpose at the moment
authors["uni_normalized"] = authors["uni_normalized"].str.strip()
authors = authors.drop(columns = {"location_normalized"})

# ### create extract last part of name for lastname, put rest into the middlename
# make the middlename as follows: if "", then " ", otherwise " name ". then paste without empty spaces
authors["middlename"] = np.where(authors["middlename"].isna(), "", authors["middlename"])
authors["middlename"] = authors["middlename"].apply(lambda s: f" {s} " if s != "" else " ") 
authors["fullname"] = authors["firstname"] + authors["middlename"] + authors["lastname"]


# ### Drop records where name = "awarding" anad middle_lastname contains "body" 
    # for instance 1780283815, 1780286903. non-useable name information; seems to be a bunch of UK universities
authors = (authors
            .loc[~( (authors["firstname"] ==  "awarding") & 
                    (authors["fullname"].str.contains("body"))
                    ),
                :]
        )
print(f"Dropped {n_authors - authors.shape[0]} authors with name = 'awarding' and middle_lastname contains 'body' ")
n_authors = authors.shape[0]

# ### University names and ids
universities = pd.Series(authors["university"].unique())
df_unis = pd.DataFrame({"university": universities})
df_unis["university_id"] = df_unis.index + 1

authors = (authors.set_index(["university"]).
           join(df_unis.set_index(["university"]), 
                on = "university").
           reset_index()
           )

# uni_names = authors.groupby("university").size().reset_index(name = "counts").sort_values(by = "counts", ascending = False)
    # NOTE: university may contain parentheses. The string in the last parenthesis seems to be country (or sometimes city)
        # the string in the preceding parenthesis, if it exists, may be specifying the name in more detail ("Paris Nanterre")
    # For now, just create a university identifier (analogue to MAG); there seem to be few duplicates, if any. 
        # For instance, I could not find any obvious duplicates with/without "The" 


# ### Clean numeric_date and degree_year. 
    # just drop all records where the publication year and the degree year do not agree 
    #   -- drops less than 0.5 percent of records (np.quantile(authors.year_diff, .998))
authors["publication_year"] = pd.to_numeric(authors.numeric_date.str.slice(start = 0, stop = 4), 
                                            errors = "coerce")

authors["year_diff"] = np.abs(authors["publication_year"] - authors["degree_year"])

authors = authors.loc[authors["publication_year"] == authors["degree_year"]]

print(f"Dropped {n_authors - authors.shape[0]} authors with non-matching degree year and publication year. \n")
n_authors = authors.shape[0]

# authors.loc[(authors.middlename != authors.new_middlename) & (~authors.new_middlename.isna()), ["firstname", "middlename", "lastname", "new_middlename"]].head()
authors = authors.drop(columns = ["middlename", "lastname"])


# ### Prepare output
unis = (authors.loc[:, ["university_id", "university", "uni_normalized", "university_location"]].
            rename(columns = {"university": "originalname", 
                              "uni_normalized": "normalizedname",
                              "university_location": "location"})
        )
unis = unis.drop_duplicates(subset = ["university_id"])
authors = authors.loc[:, ["goid", "fullname", "firstname", "degree_year", "degree_level", "university_id", "department", 
                          "originaltitle", "thesistitle"]]
advisors = advisors.loc[advisors["goid"].isin(authors["goid"])]
advisors["relationship_id"] = advisors["goid"].astype(str) + "_" + advisors["position"].astype(str) # this is the entity use for advisor linking
fields = fields.loc[fields["goid"].isin(authors["goid"]),
                    ["goid", "position", "fieldname", "fieldcode"]
                ] # there is not a unique mapping from fieldnames to fieldcodes, so just use the fieldname string

# ## Transfer to db 
print("Writing to db... \n")

db_inputs = {
    "authors": {
        "df": authors,
        "tb_name": "pq_authors",
        "schema": 
            """goid INTEGER
            , fullname TEXT
            , firstname TEXT
            , degree_year INTEGER
            , degree_level TEXT
            , university_id INTEGER
            , department TEXT
            , originaltitle TEXT
            , thesistitle TEXT""",
        "create_idx": ["CREATE UNIQUE INDEX idx_pqaut_goid ON pq_authors (goid ASC)",
                        "CREATE INDEX idx_pqaut_uni ON pq_authors (university_id ASC)",
                        "CREATE INDEX idx_pqaut_fname ON pq_authors (firstname ASC)" ]
    },
    "advisors": {
        "df": advisors,
        "tb_name": "pq_advisors",
        "schema":
            """goid INTEGER
            , position INTEGER
            , lastname TEXT
            , firstname TEXT
            , relationship_id TEXT""",
        "create_idx": ["CREATE INDEX idx_pqadv_idpos ON pq_advisors (goid ASC)",
                        "CREATE UNIQUE INDEX idx_pqadv_relid ON pq_advisors(relationship_id)"]
    },
    "fields": {
        "df": fields,
        "tb_name": "pq_fields",
        "schema": 
            """goid INTEGER
            , position INTEGER
            , fieldname TEXT
            , fieldcode INTEGER""",
        "create_idx": ["CREATE INDEX idx_pqfld_idpos ON pq_fields (goid ASC)"]
    },
    "universities": {
        "df": unis,
        "tb_name": "pq_unis",
        "schema":
            """university_id INTEGER
            , originalname TEXT
            , normalizedname TEXT
            , location TEXT
            """,
        "create_idx": ["CREATE UNIQUE INDEX idx_pqu_id ON pq_unis (university_id ASC)"]
    }
}


for k, v in db_inputs.items():
    filename = databasepath + "tempfile.csv"
    v["df"].to_csv(filename, index = False, header = False) # sqlite does not recognize header rows
    tbl = v['tb_name']
    con.execute(f"DROP TABLE IF EXISTS {tbl}")
    con.execute(f"""CREATE TABLE {tbl}( {v['schema']} )""")
    subprocess.run(
        ["sqlite3", db_file,
        ".mode csv",
        f".import {filename} {tbl}"]
    )
    for command in v["create_idx"]:
        con.execute(command)
    os.remove(filename)


# ## Analyze and finish 
analyze_db(con)

con.close()

print("Done.")



