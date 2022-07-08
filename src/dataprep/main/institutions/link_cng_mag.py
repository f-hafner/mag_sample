# %%
# ## linking institutions from mag with list of all institutions

import sqlite3 as sqlite
import argparse
import os
import pandas as pd
import numpy as np 
import dedupe
import multiprocessing as mp

from helpers.variables import db_file
from helpers.functions import analyze_db

def links_to_row(l):
    "convert linking output from dedupe to an np array"
    ids = l[0]
    score = l[1]
    out = [ids[0], ids[1], score]
    out = np.array(out)
    return(out)

def ignore_medical_institutions(s1, s2): # using this requires that one labels pairs with "no" when one has the med string but not the other
    """
    teach the model to ignore pairs 
    where only one of the pairs is a med institution and the other is not
    """
    med = ["medical", "medicine", "health", "hospital", "cancer"] 
    med_in_s1 = [x in s1 for x in med]
    med_in_s2 = [x in s2 for x in med]
    if (any(med_in_s1) and not any(med_in_s2) 
        or (any(med_in_s2) and not any(med_in_s1))
        ):
        return 1
    else:
        return 0



dedupe_datapath = "/mnt/ssd/DedupeFiles/institution_links/"
settings_file = dedupe_datapath + "mag_cng_settings"
training_file = dedupe_datapath + "mag_cng_training.json"

query_mag = """SELECT AffiliationId
                        , NormalizedName AS name
                        , PublicationCount
                        , Latitude as lat
                        , Longitude as lon
                    FROM affiliations
                    INNER JOIN affiliation_outcomes using(AffiliationId)
                    WHERE Iso3166Code = 'US' """

query_cng = """SELECT unitid
            , normalizedname AS name
            , latitude AS lat
            , longitude AS lon
            FROM cng_institutions"""


n_cores = int(mp.cpu_count() / 2)


if __name__ == "__main__":
    # %%
    # ## mag sample: Check institutions and names
    con = sqlite.connect(db_file)

    with con:
        mag = pd.read_sql(sql=query_mag, con=con)
        cng = pd.read_sql(sql=query_cng, con = con)

    con.close()


    # %% [markdown]
    # ### Prepare for dedupe
    # - Create dicts from dfs
    # - Give same names to features
    # - Set up the linker 

    # %%
    for data in [mag, cng]:
        data["location"] = data.apply(lambda row: (row["lat"], row["lon"]), axis=1)


    # %%
    magdata = mag.loc[:, ["AffiliationId", "name", "location"]].set_index("AffiliationId").to_dict(orient="index")
    cngdata = cng.loc[:, ["unitid", "name", "location"]].set_index("unitid").to_dict(orient="index")


    # %%

    if os.path.exists(settings_file):
        print('reading from', settings_file)
        with open(settings_file, 'rb') as sf:
            linker = dedupe.StaticRecordLink(sf)
    else:
        fields = [
            {"field": "name", "variable name": "name", "type": "String"},
            {"field": "location", "variable name": "location", "type": "LatLong"},
            {"type": "Interaction", "interaction variables": ["name", "location"]},
            {"field": "name", "variable name": "med_name", "type": "Custom", "comparator": ignore_medical_institutions}, 
            {"type": "Interaction", "interaction variables": ["med_name", "location"]},
            {"type": "Interaction", "interaction variables": ["med_name", "name"]}
        ]

        linker = dedupe.RecordLink(fields, num_cores = n_cores) 
        linker.prepare_training(
            data_1=magdata, data_2=cngdata, blocked_proportion=0.9, sample_size=100_000
            )

        if os.path.exists(training_file):
            print(f"reading training data from {training_file}")
            with open(training_file) as tf:
                linker.prepare_training(
                    magdata, cngdata, training_file=tf, sample_size=15000
                )
        else:
            linker.prepare_training(magdata, cngdata, sample_size=15000)

        print("Starting active labeling", flush=True)
        dedupe.console_label(linker)

        linker.train()

        with open(training_file, "w") as tf: 
            linker.write_training(tf)
                
        with open(settings_file, "wb") as sf: 
            linker.write_settings(sf)

        linker.cleanup_training()


    print("Clustering")

    linked_records = linker.join(magdata, cngdata, threshold=0.0, constraint="one-to-one")

    linked_records = [links_to_row(i) for i in linked_records]
    out = pd.DataFrame(linked_records, columns=["AffiliationId", "unitid", "link_score"])

    out["datasets"] = "mag_cng"

    con = sqlite.connect(db_file)
    with con: 
        # delete the links between these data sets if we have some already
        c = con.cursor()
        c.execute("SELECT COUNT(name) FROM sqlite_master WHERE type = 'table' AND name = 'institution_links' ")

        if c.fetchone()[0] == 1:
            con.execute("DELETE FROM institution_links WHERE datasets = 'mag_cng' ")

        # write to db
        out.to_sql("institution_links", 
                    con=con, 
                    if_exists="append", 
                    index=False, 
                    chunksize=out.shape[0]
                    )

        analyze_db(con)


    con.close()


    print("Done")