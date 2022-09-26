
"""
Write temporary links from csv to database

- Read files from /mnt/ssd/linking_ids_temp/ for linking_type and train_name
- Check if they exist in the database already 
- If they do not, append links and linking_info to tables linked_ids* and linking_info*
"""


import numpy as np
import argparse
import pandas as pd
import time 
import sqlite3 as sqlite
import os 
import pdb 
import re 

from helpers.variables import db_file, datapath
from helpers.functions import analyze_db

# just a note for the moment: the max iteration id was 43 before I started working on this file 


# register [adapter for numpy.int64](https://stackoverflow.com/questions/38753737/inserting-numpy-integer-types-into-sqlite-with-python3)
sqlite.register_adapter(np.int64, lambda val: int(val))
sqlite.register_adapter(np.int32, lambda val: int(val))
sqlite.register_adapter(np.float64, lambda val: float(val))
sqlite.register_adapter(np.float32, lambda val: float(val))

# ## Arguments
parser = argparse.ArgumentParser(description='Inputs for writing csv links')
parser.add_argument("--linking_type", type=str, default="graduates",
                    help = "Are we linking graduates or advisors?",
                    choices = {"graduates", "advisors", "grants"}) 
parser.add_argument("--train_name", type=str, default="christoph_fielddegree0",
                    help="Training name used for making the links")
args = parser.parse_args()

# ## Other parameters
temp_data_path = datapath+"linked_ids_temp/"
tbl_links = "linked_ids"
tbl_info = "linking_info"
start_time = time.time()


if args.linking_type != "graduates":
    tbl_links = f"{tbl_links}_{args.linking_type}"
    tbl_info = f"{tbl_info}_{args.linking_type}"

files = os.listdir(temp_data_path)
 
files = [f for f in files 
            if args.linking_type in f 
            if args.train_name in f]

link_files = [f for f in files if "links_" in f]
info_files = [f for f in files if "linking_info" in f]
fields = [re.sub(f"links_{args.linking_type}_|.csv|_{args.train_name}", "", x) for x in link_files]

dict_to_read = {f: {
                "links": [file for file in link_files if f in file][0] ,
                "info": [file for file in info_files if f in file][0]
                }
            for f in fields
}


with sqlite.connect(db_file) as con:
    d_iterations = pd.read_sql(sql=f"SELECT * FROM {tbl_info}", con=con)



# need string types for some of the binary columns in linking_info
strvars = ["institution", "fieldofstudy_cat", "fieldofstudy_str", "keywords"]
dtypes_info = {i: str for i in strvars}

index_cols = [i for i in d_iterations.columns if i != "iteration_id"]

iteration_counter = 0 # to give each field/file a distinct iteration id 
for field, files in dict_to_read.items():
    #links = pd.read_csv(f"{temp_data_path}{files['links']}")
    data = {k: pd.read_csv(f"{temp_data_path}{v}", dtype=dtypes_info) for k, v in files.items()}
    # check if the iteration already exists
    check_row = (d_iterations
                    .set_index(index_cols)
                    .join(data["info"]
                            .set_index(index_cols), 
                            how="inner")
                    .reset_index())
    if check_row.shape[0] > 0:
        print(f"The links of the iteration of field {field} are already in the database.")
    else:
        iteration_counter = iteration_counter + 1 
        print(f"Writing field {field}")
        iter_id = d_iterations.iteration_id.max() + iteration_counter
        for df in data.values():
            df["iteration_id"] = iter_id

        
        with sqlite.connect(db_file) as con:
            data["info"].to_sql(name=tbl_info, con=con, if_exists="append", index=False)
            data["links"].to_sql(name=tbl_links, con=con, if_exists="append", index=False)

        

with sqlite.connect(db_file) as con:
    analyze_db(con)


time_to_run = (time.time() - start_time)/60

print(f"Done in {time_to_run} minutes.")



