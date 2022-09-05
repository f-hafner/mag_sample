
"""
Link MAG institutions to carnegie classification.
"""

import sqlite3 as sqlite
import argparse
import os
import pandas as pd
import dedupe
import multiprocessing as mp

from helpers.variables import db_file
from main.institutions.utils import dedupe_datapath, links_to_row
import main.institutions.sql_queries as sq
from main.institutions.dedupe_setup import fields_mag

dedupe_sample_size = 100_000 
dedupe_share_blockedpairs = 0.7

settings_file = dedupe_datapath + "mag_settings"
training_file = dedupe_datapath + "mag_training.json"

parser = argparse.ArgumentParser()
parser.add_argument("--tofile", 
                    type=str,
                    default="../../data/link_institutions/links_mag.csv",
                    help="file path and name to temporarily save the links.") 
args = parser.parse_args()

n_cores = int(mp.cpu_count() / 2)

def main():
     # ## mag sample: Check institutions and names
    con = sqlite.connect(db_file)

    with con:
        mag = pd.read_sql(sql=sq.query_mag, con=con)
        cng = pd.read_sql(sql=sq.query_cng, con=con)

    con.close()

    # ## 1. link on exact name (unique names in cng only)
    cng_dupes = (cng
                    .groupby("name")["unitid"]
                    .count()
                    .reset_index()
                    .rename(columns={"unitid": "nb"})
                    )
    cng_dupes = cng_dupes.loc[cng_dupes["nb"] > 1] 
    cng_nodupes = cng.loc[~cng["name"].isin(cng_dupes["name"])]

    mag_dupes = (mag
                    .groupby("name")["AffiliationId"]
                    .count()
                    .reset_index()
                    .rename(columns={"AffiliationId": "nb"})
    )
    mag_dupes = mag_dupes.loc[mag_dupes["nb"] > 1]
    mag_nodupes = mag.loc[~mag["name"].isin(mag_dupes["name"])]

    links_name = (cng_nodupes
            .loc[:, ["unitid", "name"]]
            .set_index("name")
            .join(
                mag_nodupes
                .loc[:, ["AffiliationId", "name"]]
                .set_index("name"),
                how="left"
                )
            .reset_index("name")
        )
    mask = ~links_name["AffiliationId"].isna()
    keep_cols = ["AffiliationId", "unitid"]
    links_name = links_name.loc[:, keep_cols]
    links_name = links_name.loc[mask, :]
    links_name["link_score"] = 1
    links_name["link_flag"] = "exact_unique_name"

    mag = mag.loc[~mag["AffiliationId"].isin(links_name["AffiliationId"]), :]
    cng = cng.loc[~cng["unitid"].isin(links_name["unitid"]), :]

    # ## 2. Dedupe
    # - Create dicts from dfs
    # - Give same names to features
    # - Set up the linker 
    for data in [mag, cng]:
        data["location"] = data.apply(lambda row: (row["lat"], row["lon"]), axis=1)

    magvars = ["AffiliationId", "name", "location"]
    cngvars = ["unitid", "name", "location"]
    data1 = (mag
                .loc[:, magvars]
                .set_index("AffiliationId")
                .to_dict(orient="index"))
    data2 = (cng
                .loc[:, cngvars]
                .set_index("unitid")
                .to_dict(orient="index"))

    colnames_link = [v[0] for v in [magvars, cngvars]]

    if os.path.exists(settings_file):
        print('reading from', settings_file)
        with open(settings_file, 'rb') as sf:
            linker = dedupe.StaticRecordLink(sf)
    else:
        linker = dedupe.RecordLink(fields_mag, num_cores = n_cores) 
        linker.prepare_training(
            data_1=data1, 
            data_2=data2, 
            blocked_proportion=dedupe_share_blockedpairs, 
            sample_size=dedupe_sample_size
        )

        if os.path.exists(training_file):
            print(f"reading training data from {training_file}")
            with open(training_file) as tf:
                linker.prepare_training(
                    data1, data2, training_file=tf, sample_size=dedupe_sample_size
                )
        else:
            linker.prepare_training(data1, data2, sample_size=dedupe_sample_size)

        print("Starting active labeling", flush=True)
        dedupe.console_label(linker)

        linker.train()

        with open(training_file, "w") as tf: 
            linker.write_training(tf)
                
        with open(settings_file, "wb") as sf: 
            linker.write_settings(sf)

        linker.cleanup_training()


    print("Clustering")

    linked_records = linker.join(data1, data2, threshold=0.0, constraint="one-to-one")

    linked_records = [links_to_row(i) for i in linked_records]
    colnames_link.append("link_score")
    out = pd.DataFrame(linked_records, columns=colnames_link)
    out["link_flag"] = "dedupe"

    out = pd.concat([out, links_name]) # make sure they are in the same order
    out = out.rename(columns = {"AffiliationId": "from_id"})
    out["from_dataset"] = "mag"

    out.to_csv(args.tofile, index=False)

    print("Done")


if __name__ == "__main__":
    main()
   