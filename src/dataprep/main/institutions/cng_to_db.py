"""
Write carnegie institutions to database
- import institutions and filter relevant entities
- add lat/lon from zip codes
- save to database
"""

# %%

import sqlite3 as sqlite
import pandas as pd
import numpy as np
import re 
import argparse 


from helpers.functions import normalize_string, analyze_db
from helpers.variables import db_file
from helpers.us_states import us_states 

parser = argparse.ArgumentParser()
parser.add_argument("--rawdata", 
                    type=str,
                    default="../../data/institutions",
                    help="path to raw data with files for zip codes and carnegie classification") 
parser.add_argument("--zipcodes", 
                    type=str,
                    default="ZIP_codes_2020.xls",
                    help="name of file with zip codes") 
parser.add_argument("--cng", 
                    type=str,
                    default="CCIHE2021-PublicData.xlsx",
                    help="name of file with carnegie classification") 
                    
                    
args = parser.parse_args()


# ## Define some replacements in cng institutions


replace_city_names = { # facilitate merge with zip code file
    "Normal, AL": "Huntsville",
    "Saint Leo, FL": "Dade City",
    "Alcorn State, MS": "Alcorn State University",
    "Buies Creek, NC": "Lillington",
    "Tigerville, SC": "Marietta",
    "McKenzie, TN": "Paris",
    "Emory, VA": "Meadowview",
    "Franklin Springs, GA": "Royston",
    "Toccoa Falls, GA": "Toccoa",
    "St. Mary's City, MD": "Saint Marys City",
    "Sault Ste Marie, MI": "Sault Sainte Marie",
    "St. Cloud, MN": "Saint Cloud",
    "Point Lookout, MO": "Hollister",
    "Brooklyn Heights, NY": "Brooklyn",
    "La Plume, PA": "Factoryville"
}

replace_uni_names = { # facilatate linking to mag/pq
    196060: "university at albany suny",
    231624: "college of william mary",
    207388: "oklahoma state university stillwater",
    190576: "city university of new york", #https://en.wikipedia.org/wiki/Graduate_Center,_CUNY
    196033: "state university of new york system", # this seems to be the closest one in MAG 
    233921: "virginia tech",
    236948: "university of washington", # note: this will lead to an exact link; but MAG only has one univ of wash.
    218663: "university of south carolina",
    126818: "colorado state university",
    221759: "university of tennessee",
    190567: "city college of new york",
    126562: "university of colorado denver",
    215293: "university of pittsburgh",
    484613: "university of phoenix"
}

# drop these states and online universities
drop_states = ["PR", "GU", "VI", "MP"]
drop_online = "online|digital immersion|global campus|worldwide"
# keep these basic2021 codes (doctoral unis, master's and baccalaureate colleges)
keep_cng_levels = list(range(15, 24))
keep_cng_levels.append(27) #27 = "special four-year: research institution"


# %%
# ## 1. Prepare data

print("Reading cng file...", flush=True)

cng = pd.read_excel(args.rawdata + "/" + args.cng,
                    sheet_name="Data",
                    usecols=["unitid", "name", "city", "stabbr", "iclevel", "basic2021"],
                    header=0)


print("Cleaning...", flush=True)

cng["normalizedname"] = normalize_string(cng["name"], replace_hyphen = " ")
mask = (cng["iclevel"] == 1) \
        & (~cng["stabbr"].isin(drop_states)) \
        & (~cng["normalizedname"].str.contains(drop_online)) \
        & (cng["basic2021"].isin(keep_cng_levels))


cng = cng.loc[mask, :]
cng = cng.drop(columns = ["iclevel"]).rename(columns = {"name": "originalname"})


# %%

# ### Change some institution names in cng for better link to MAG
for k, v in replace_uni_names.items():
    cng["normalizedname"] = np.where(cng["unitid"] == k, v, cng["normalizedname"])


# ### change some city names in cng_institutions to merge with city names
    # some of these names do not have a zip code, or missing lat/lon in the zip code files
cng["city_state"] = cng['city'] + ", " + cng['stabbr']
for k, v in replace_city_names.items(): 
    cng["city"] = np.where(cng['city_state'] == k, v, cng["city"])

cng = cng.drop(columns="city_state")
cng["city"] = normalize_string(cng["city"], replace_hyphen= " ")

cng["normalizedname"] = cng["normalizedname"].str.removeprefix("the ")
#cng["normalizedname"] = cng["normalizedname"].str.removesuffix(" main campus")
cng["normalizedname"] = cng["normalizedname"].str.removesuffix(" campus immersion")
cng["normalizedname"] = cng["normalizedname"].str.strip()

mask = ~cng["normalizedname"].str.contains("adult degree|lifelong learning|continuing professional|national global")
cng = cng.loc[mask, :]


# ### change name for linking to MAG: MAG seems to have one affiliation for these places (but a few publications are also recorded for the other places)
    # assign the same name to all these ones here; let dedupe find the best link based on location
    # this means we lose the other colleges, but they are less important for publications
# mask = cng["normalizedname"].str.startswith("cuny ")
# cng["normalizedname"] = np.where(mask, "city university of new york", cng["normalizedname"])


# %% 
# ## 2. Load and prepare zip code data 
# - add stabbr
# - drop duplicated names within state: keep largest 
# - merge on stabbr and city name
# - do second merge with alternative names reported in csv files

# %%
print("Loading zip code file...", flush=True)
vars_mcdc = ["ZIP Code", "Type", "State FIPS", "Preferred name", 
            "Alternate names", "Latitude", "Longitude", "Population (2020)"]
mcdc = pd.read_excel(args.rawdata + "/" + args.zipcodes, 
                        usecols = vars_mcdc)
mcdc.columns = ["zipcode", "type", "statefips", "name", "altername",
                "pop", "lat", "lon"]

mask = (mcdc["statefips"] != 72) & (~mcdc.lat.isna())
mcdc = mcdc.loc[mask, :]

mcdc = (mcdc
        .set_index("statefips")
        .join(us_states.loc[:, ["fips", "abbr"]]
            .set_index("fips"))
        .reset_index()
        .rename(columns={"index": "statefips"})
        )

assert mcdc.loc[mcdc.abbr.isna(), :].shape[0] == 0


# %%
# ### Keep exploded df for second round of merging (some places in cng have the alternate name from zip code files)
mcdc_expl = mcdc.copy()
mcdc_expl["altername"] = mcdc_expl["altername"].str.split(",")
mcdc_expl = mcdc_expl.explode("altername")


mcdc_expl["altername"] = normalize_string(mcdc_expl["altername"], replace_hyphen= " ")
mcdc_expl["max_pop"] = mcdc_expl.groupby(["statefips", "altername"])["pop"].transform("max")
mcdc_expl = mcdc_expl.loc[mcdc_expl["max_pop"] == mcdc_expl["pop"], :]


# %%
# ### Keep unique city names within state
mcdc["city"] = mcdc.apply(lambda row: re.sub(row['abbr'], "", row["name"]).strip(), axis="columns")
mcdc["city"] = normalize_string(mcdc["city"], replace_hyphen= " ")
mcdc["max_pop"] = mcdc.groupby(["statefips", "city"])["pop"].transform("max")
mcdc = mcdc.loc[mcdc["max_pop"] == mcdc["pop"], :]

# %% 
# ## 3. Add zip code data based on name and state
    # 1. match on city name and state
    # 2. match remaining on alternative city name and state 

# %%

print("Merging zip code lat/long data to cng, round 1...", flush=True)

cng = (cng
        .set_index(["stabbr", "city"])
        .join(mcdc.loc[:, ["abbr", "city", "lat", "lon"]]
            .rename(columns = {"abbr": "stabbr"})
            .set_index(["stabbr", "city"])
        )
        .reset_index()
    )


print("... round 2", flush=True)
mask = cng["lat"].isna()
cng_missing = cng.loc[mask, :]
cng_missing = (cng_missing
                .drop(columns=["lat", "lon"])
                .set_index(["stabbr", "city"])
                .join(mcdc_expl.loc[:, ["abbr", "altername", "lat", "lon"]]
                        .rename(columns={"abbr": "stabbr", "altername": "city"})
                        .set_index(["stabbr", "city"]))
                .reset_index()
            )

assert cng_missing.loc[cng_missing.lat.isna()].shape[0] == 0


# %%
# ### prepare final output

print("Making output and writing to db...", flush=True)

to_concat = [
    cng.loc[~cng["lat"].isna(), :],
    cng_missing
]
cng_out = pd.concat(to_concat, axis=0)

assert cng_out.shape == cng.shape
assert len(cng_out.unitid.unique()) == cng_out.shape[0]

cng_out = cng_out.rename(columns = {"lat": "latitude", "lon": "longitude"})
reorder_cols = ["unitid", "normalizedname", "originalname", "city", "stabbr", "basic2021", "latitude", "longitude"]
cng_out = cng_out[reorder_cols]

# %% 
# ## 4. Write to db

con = sqlite.connect(db_file)
with con: 
    cng_out.to_sql("cng_institutions", 
                    con=con, 
                    if_exists="replace", 
                    index=False, 
                    chunksize=cng_out.shape[0]
                )

    con.execute("CREATE UNIQUE INDEX idx_cngi_unitid ON cng_institutions (unitid ASC)")

    analyze_db(con)

con.close()


# %%
print("Done.")


