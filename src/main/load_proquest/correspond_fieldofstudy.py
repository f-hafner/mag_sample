#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Correspond field of study in MAG and in ProQuest

Assign field 0 and field1 to each author; field0 needs to match MAG, and field1 if possible as well. 

Three ways to define field0:
    - direct match in Web of Science classification (level 0 or 1; priority to level 1 b/c more information for linking). Then link to MAG with our crosswalk. 
    - match in MAG level 0-2. Then use the MAG crosswalk_fields to correspond to MAG level0. 
    - match in MAG level 0 or Web of Science level0 based on the last part of the fieldname reported ("ancient history" = "history").

fieldname is always the reported fieldname used for assigning field0.
mag_field0 is the FieldOfStudyId in MAG at Level 0.
flag indicates how field0 was defined.

"""

# TODO
    # do matching: on fields; conservative on missing first name; on second part of name instead of fullname

import sqlite3 as sqlite
import argparse
import os
import subprocess
import pandas as pd
import numpy as np

from helpers.variables import db_file, datapath, databasepath
from helpers.functions import analyze_db

con = sqlite.connect(database = db_file, isolation_level= None)

def keep_min_position(df, keep_cols = ["goid", "position", "fieldname"]):
    """For set of records, keep only the one with the lowest position per goid"""
    df["min_position"] = df["position"].groupby(df["goid"]).transform("min")
    df["keep"] = np.where(df["min_position"] == df["position"], 1, 0)
    df = df.loc[df["keep"] == 1, keep_cols]
    return df


# ## Read inputs

pq_fields = pd.read_sql(sql = "SELECT * FROM pq_fields", con = con)

fos_wos = pd.read_csv(datapath + "Misc/wos_categories.csv")
fos_mag = pd.read_sql("SELECT FieldOfStudyId, NormalizedName, Level FROM FieldsOfStudy WHERE Level <= 1", con)  ##**## NOTE: can try to increase to level 2, but it would not add many more graduates
fos_mag_hierarchy = pd.read_sql("SELECT ParentFieldOfStudyId, ChildFieldOfStudyId, ParentLevel, ChildLevel FROM crosswalk_fields", con)
crosswalk_wos_mag = pd.read_csv(datapath + "Misc/crosswalk_wos_mag.csv")
mag_fields0 = pd.read_sql("SELECT FieldOfStudyId, NormalizedName FROM FieldsOfStudy WHERE Level = 0", con)


# ### MAG has several fields with the same name -- keep only the ones that are unique 
fos_mag["nb"] = fos_mag.groupby(["NormalizedName"])["FieldOfStudyId"].transform("count")
fos_mag = fos_mag.loc[fos_mag["nb"] == 1].drop(columns = ["nb"]).copy()

assert fos_wos[fos_wos.duplicated("category")].shape[0] == 0
assert fos_mag[fos_mag.duplicated("NormalizedName")].shape[0] == 0


# ## Correspond unique fields 

fields = pq_fields.loc[~pq_fields.duplicated("fieldname"), ["fieldname"]]

# ### (1a) Directly WOS, level 0
fields_wos_lvl1 = fields.loc[pq_fields["fieldname"].isin(fos_wos["category"])].copy()
n_start = fields_wos_lvl1.shape[0]

fields_wos_lvl1 = (fields_wos_lvl1
            .set_index(["fieldname"])
            .join(fos_wos.loc[:, ["field_wos", "category"]]
                    .set_index(["category"]))
            .reset_index()
            .rename(columns = {"index": "fieldname"})
            .set_index(["field_wos"])
            .join(crosswalk_wos_mag.loc[:,["wos_id", "field_mag"]]
                    .set_index(["wos_id"]))
            .reset_index()
            .rename(columns = {"index": "field_wos"})
            .loc[:, ["fieldname", "field_mag"]]
            .set_index(["field_mag"])
            .join(mag_fields0
                    .set_index(["NormalizedName"]))
            .reset_index()
            .rename(columns = {"index": "field_mag"})
            .rename(columns = {"fieldname": "field_proquest",
                                "field_mag": "field0",
                                "FieldOfStudyId": "mag_field0"})
            .drop(columns = ["field0"])
            )
fields_wos_lvl1["flag"] = "oecd_wos_lvl1"

assert n_start == fields_wos_lvl1.shape[0]

# ### (1b) Directly WOS, level 1 
fields_wos_lvl0 = fields.loc[(pq_fields["fieldname"].isin(crosswalk_wos_mag["field_wos"]) &
                                (~pq_fields["fieldname"].isin(fields_wos_lvl1["field_proquest"]))
                                )                            
                            ].copy()


n_start = fields_wos_lvl0.shape[0]

fields_wos_lvl0 = (fields_wos_lvl0
                    .set_index(["fieldname"])
                    .join(crosswalk_wos_mag.loc[:, ["field_wos", "field_mag"]].
                            set_index(["field_wos"]))
                    .reset_index()
                    .rename(columns = {"index": "fieldname"})
                    .set_index(["field_mag"])
                    .join(mag_fields0
                        .set_index(["NormalizedName"]))
                    .reset_index()
                    .rename(columns = {"index": "field_mag"})
                    .rename(columns = {"fieldname": "field_proquest",
                                        "field_mag": "field0",
                                        "FieldOfStudyId": "mag_field0"})
                    .drop(columns = ["field0"])
                    )

fields_wos_lvl0["flag"] = "oecd_wos_lvl0"

assert n_start == fields_wos_lvl0.shape[0]


# ### (2) MAG categories
fields_mag = fields.loc[(~fields["fieldname"].isin(fields_wos_lvl1["field_proquest"]) & 
                            ~fields["fieldname"].isin(fields_wos_lvl0["field_proquest"]) &
                            fields["fieldname"].isin(fos_mag["NormalizedName"])
                        )
                        ].copy()

n_start = fields_mag.shape[0]

fields_mag = (fields_mag
                .set_index(["fieldname"])
                .join(fos_mag.loc[:, ["FieldOfStudyId", "NormalizedName"]]
                        .set_index(["NormalizedName"]))
                .reset_index()
                .rename(columns = {"index": "fieldname"})
                .set_index(["FieldOfStudyId"])
                .join(fos_mag_hierarchy.loc[fos_mag_hierarchy["ParentLevel"] == 0,["ParentFieldOfStudyId", "ChildFieldOfStudyId"]]
                        .set_index(["ChildFieldOfStudyId"])
                    )
                .reset_index()
                .rename(columns = {"index": "FieldOfStudyId"})
            )
fields_mag["flag"] = np.where(fields_mag["ParentFieldOfStudyId"].isna(), "mag_lvl0", "mag_lvl1")
fields_mag["ParentFieldOfStudyId"] = np.where(fields_mag["ParentFieldOfStudyId"].isna(), fields_mag["FieldOfStudyId"], fields_mag["ParentFieldOfStudyId"])
assert fields_mag[fields_mag.ParentFieldOfStudyId.isna()].shape[0] == 0

fields_mag = (fields_mag.loc[:, ["fieldname", "ParentFieldOfStudyId", "flag"]]
            .rename(columns = {"fieldname": "field_proquest",
                               "ParentFieldOfStudyId": "mag_field0"})
         )
assert fields_mag.shape[0] == n_start


# ### (3) Use words where the last part falls into one of the follow fields: (I manually checked, they all make sense)
    # ("literature", "history", "psychology", "medicine", "physics", "engineering", "mathematics", "philosophy", "chemistry", "geology")
    # literature:s level 1, not 0

fields_to_match = ["literature", "history", "psychology", "medicine", "physics", "engineering", "mathematics", 
                    "philosophy", "chemistry", "geology"]

fields_end = fields.loc[(~fields["fieldname"].isin(fields_wos_lvl0["field_proquest"]) &
                            (~fields["fieldname"].isin(fields_wos_lvl1["field_proquest"])) &
                            (~fields["fieldname"].isin(fields_mag["field_proquest"]))
                            )
                        ].copy()

fields_end["name_split"] = fields_end["fieldname"].map(lambda x: x.split(" "))
fields_end["last_part"] = fields_end["name_split"].map(lambda x: x[-1])
fields_end = fields_end.loc[fields_end["last_part"].isin(fields_to_match), 
                             ["fieldname", "last_part"]
                             ].copy()

n_start = fields_end.shape[0]

# #### Add field_wos id to those where it matches (mostly literature)
fields_end = (fields_end
                .set_index("last_part")
                .join(fos_wos.loc[:, ["field_wos", "category"]]
                    .set_index(["category"]))
                .reset_index()
                .rename(columns = {"index": "last_part"})
                .set_index(["field_wos"])
                .join(crosswalk_wos_mag.loc[:,["wos_id", "field_mag"]]
                        .set_index(["wos_id"]))
                .reset_index()
                .rename(columns = {"index": "field_wos"})
                )

# #### Those with missing field_mag: their last_part is the field_mag at level0, so assign this
fields_end["field_mag"] = np.where(fields_end["field_mag"].isna(),
                                    fields_end["last_part"],
                                    fields_end["field_mag"])


# #### Now add the mag id 
fields_end = (fields_end
                .set_index(["field_mag"])
                .join(mag_fields0
                .set_index(["NormalizedName"]))
                .reset_index()
                .rename(columns = {"index": "field_mag"})
                .loc[:, ["fieldname", "FieldOfStudyId"]]
                .rename(columns = {"fieldname": "field_proquest",
                                    "FieldOfStudyId": "mag_field0"})
                )
fields_end["flag"] = "lvl0_on_endofstring"

assert n_start == fields_end.shape[0]


# ### (4) Add everything together
fields_matched = pd.concat((fields_wos_lvl0, fields_wos_lvl1, 
                            fields_mag, fields_end)
                            )

# ## Link to authors; keep multiple; investigate fields of missing authors 
authors_fields = (pq_fields
                    .loc[pq_fields["fieldname"].isin(fields_matched["field_proquest"]),
                            ["goid", "position", "fieldname"]
                        ]
                    .set_index(["fieldname"])
                    .join(fields_matched.loc[:, ["field_proquest", "mag_field0", "flag"]]
                            .set_index(["field_proquest"]))
                    .reset_index()
                    .rename(columns = {"index": "fieldname"})
                    .loc[:, ["goid", "position", "fieldname", "mag_field0", "flag"]]
                )


# ## Make some stats -- note: there will still be some fields for the matched authors for which there is no correspondence, but that is second-order 
n_matched = len(authors_fields["goid"].unique())
n_authors = len(pq_fields["goid"].unique())

print(f"Matched {n_matched / n_authors} of authors to a field0.")

unmatched_authors = pq_fields.loc[~pq_fields["goid"].isin(authors_fields["goid"])]
unmatched_fields = (unmatched_authors.groupby("fieldname")
                        .size()
                        .reset_index(name = "counts")
                        .sort_values(by = "counts", ascending = False)
                    )

print(f"50 most common fields among authors not matched to MAG: \n {unmatched_fields[0:50]}")


# ## Output

# unique goid-field0 combination
    # want to use all the fields for which a link was found? or all the rest except the one used for matching (as for the fullname/firstname problem)

authors_fields["min_position"] = authors_fields.groupby(["goid", "mag_field0"])["position"].transform("min")

# ## Write to db
authors_fields = authors_fields.loc[authors_fields["position"] == authors_fields["min_position"], 
                                    ["goid", "position", "fieldname", "mag_field0", "flag"]
                                ]

filename = databasepath + "tempfile.csv"
authors_fields.to_csv(filename, index = False, header = False)


con.execute("DROP TABLE IF EXISTS pq_fields_mag")
con.execute("""CREATE TAbLE pq_fields_mag (
        goid INT
        , position INT
        , fieldname TEXT
        , mag_field0 INT
        , flag TEXT)
    """)

subprocess.run(
    ["sqlite3", db_file,
    ".mode csv",
    f".import {filename} pq_fields_mag"]
)

con.execute("CREATE UNIQUE INDEX idx_pqfm_id on pq_fields_mag (goid ASC, mag_field0 ASC)")
con.execute("CREATE INDEX idx_pqfm_fos on pq_fields_mag (mag_field0 ASC)")


os.remove(filename)


# ## Analyze and finish 
analyze_db(con)

con.close()

print("Done.")


