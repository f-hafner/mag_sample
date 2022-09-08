# Calculate distance between cng institutions

import sqlite3 as sqlite
import pandas as pd
import numpy as np 
from geopy import distance
import os

print(os.getcwd())

from helpers.variables import db_file
import main.institutions.sql_queries as sq
from helpers.functions import analyze_db


con = sqlite.connect(db_file)

with con:
    cng = pd.read_sql(sql=sq.query_cng, con=con)

con.close()

cng = cng[["unitid", "lat", "lon"]]
cng_rhs = cng.copy(deep=True)
cng_rhs.rename(columns={"unitid":"unitid2", "lat":"lat2", "lon":"lon2"}, inplace=True)
cng = cng.merge(cng_rhs, how="cross")

print(cng.head())

# calculate distance
cng['distance_km'] = cng.apply(lambda row: distance.distance((row['lat'], row['lon']), (row['lat2'], row['lon2'])).km, axis=1)
print(cng.head())

cng_out = cng[['unitid','unitid2','distance_km']]


con = sqlite.connect(db_file)
with con: 
    cng_out.to_sql("cng_distances", 
                    con=con, 
                    if_exists="replace", 
                    index=False, 
                    chunksize=cng_out.shape[0]
                )

    con.execute("CREATE UNIQUE INDEX idx_cngd_unitid ON cng_distances (unitid ASC, unitid2 ASC)")

    analyze_db(con)

con.close()
