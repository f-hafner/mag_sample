"""
Link proquest institutions to carnegie classification.
    Only links to institutions that have been linked to mag already.
"""

import sqlite3 as sqlite
import argparse
import os
import pandas as pd
import dedupe
import multiprocessing as mp

from helpers.variables import db_file
from main.institutions.utils import dedupe_datapath, links_to_row
from main.institutions.dedupe_setup import fields_pq
import main.institutions.sql_queries as sq

parser = argparse.ArgumentParser()
parser.add_argument("--tofile", 
                    type=str,
                    default="../../data/link_institutions/links_pq.csv",
                    help="file path and name to temporarily save the links.") 
parser.add_argument("--minmag", 
                    type=float,
                    default=0.6,
                    help="Min score of link to mag from cng") 
parser.add_argument("--maglinks", 
                    type=str,
                    default="../../data/link_institutions/links_mag.csv",
                    help="Input file with links to mag") 
args = parser.parse_args()


settings_file = dedupe_datapath + "pq_settings"
training_file = dedupe_datapath + "pq_training.json"

dedupe_sample_size = 100_000 
dedupe_share_blockedpairs = 0.9


n_cores = int(mp.cpu_count() / 2)


if __name__ == "__main__":
    # ## mag sample: Check institutions and names
    con = sqlite.connect(db_file)

    with con:
        pq = pd.read_sql(sql=sq.query_pq, con=con)
        cng = pd.read_sql(sql=sq.query_cng, con = con)

    con.close()

    # condition on cng institutions with sufficiently plausible link to mag
    links_mag = pd.read_csv(args.maglinks)
    mask = links_mag["link_score"] >= args.minmag
    links_mag = links_mag.loc[mask, :]
    cng = cng.loc[cng["unitid"].isin(links_mag["unitid"]), :]

    # ## 1. match on exact name within state 
    cng_dupes = (cng
                    .groupby(["name", "stabbr"])["unitid"]
                    .count()
                    .reset_index()
                    .rename(columns={"unitid": "nb"})
                    )
    cng_dupes = cng_dupes.loc[cng_dupes["nb"] > 1] 
    for d in [cng, cng_dupes]:
        d["name_state"] = d["name"] + "_" + d["stabbr"]
    
    cng_nodupes = cng.loc[~cng["name_state"].isin(cng_dupes["name_state"])]


    pq_dupes = (pq
                .groupby(["name", "stabbr"])["university_id"]
                .count()
                .reset_index()
                .rename(columns={"university_id": "nb"})
    )
    pq_dupes = pq_dupes.loc[pq_dupes["nb"] > 1]
    for d in [pq, pq_dupes]:
        d["name_state"] = d["name"] + "_" + d["stabbr"]

    pq_nodupes = pq.loc[~pq["name_state"].isin(pq_dupes["name_state"])]

    if pq_dupes.shape[0] == 0 and cng_dupes.shape[0] == 0:
        print("No duplicates in either dataset.")

    links_name = (cng_nodupes
            .loc[:, ["unitid", "name", "stabbr"]]
            .set_index(["name", "stabbr"])
            .join(
                pq_nodupes
                .loc[:, ["university_id", "name", "stabbr"]]
                .set_index(["name", "stabbr"]),
                how="left"
                )
            .reset_index()
        )
    
    mask = ~links_name["university_id"].isna()
    keep_cols = ["university_id", "unitid"]
    links_name = links_name.loc[mask, keep_cols]
    links_name["link_score"] = 1
    links_name["link_flag"] = "exact_unique_name_state"

    pq = pq.loc[~pq["university_id"].isin(links_name["university_id"]), :]
    cng = cng.loc[~cng["unitid"].isin(links_name["unitid"]), :]

    # ## 2. Dedupe
    pqdata = pq.loc[:, ["university_id", "name", "stabbr", "city"]].set_index("university_id").to_dict(orient="index")
    cngdata = cng.loc[:, ["unitid", "name", "stabbr", "city"]].set_index("unitid").to_dict(orient="index")

    if os.path.exists(settings_file):
        print('reading from', settings_file)
        with open(settings_file, 'rb') as sf:
            linker = dedupe.StaticRecordLink(sf)
    else:
        linker = dedupe.RecordLink(fields_pq, num_cores = n_cores) 
        linker.prepare_training(
            data_1=pqdata,
            data_2=cngdata, 
            blocked_proportion=dedupe_share_blockedpairs, 
            sample_size=dedupe_sample_size
            )

        if os.path.exists(training_file):
            print(f"reading training data from {training_file}")
            with open(training_file) as tf:
                linker.prepare_training(
                    pqdata, 
                    cngdata, 
                    training_file=tf, 
                    sample_size=dedupe_sample_size
                )
        else:
            linker.prepare_training(
                pqdata, 
                cngdata,
                sample_size=dedupe_sample_size
            )

        print("Starting active labeling", flush=True)
        dedupe.console_label(linker)

        linker.train()

        with open(training_file, "w") as tf: 
            linker.write_training(tf)
                
        with open(settings_file, "wb") as sf: 
            linker.write_settings(sf)

        linker.cleanup_training()


    print("Clustering")

    linked_records = linker.join(pqdata, cngdata, threshold=0.0, constraint="one-to-one")

    linked_records = [links_to_row(i) for i in linked_records] # TODO: rename AffiliationId to "other_id" or something-> use across data sets!
    out = pd.DataFrame(linked_records, columns=["university_id", "unitid", "link_score"])
    out["link_flag"] = "dedupe"

    out = pd.concat([out, links_name]) # make sure they are in the same order
    out = out.rename(columns = {"university_id": "from_id"})
    out["from_dataset"] = "pq"

    out.to_csv(args.tofile, index=False)

    print("Done")