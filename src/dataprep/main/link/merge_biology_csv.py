# script to load separate csvs and merge them
# not generalized for now because it was an exception that it was neccessary.

import pandas as pd
from helpers.variables import datapath
import os

df1 = pd.read_csv(datapath+"archive_linked_ids_temp/linking_info_advisors_biology_christoph_degree0_20002022.csv")
df2 = pd.read_csv(datapath+"archive_linked_ids_temp/linking_info_advisors_biology_christoph_degree0_19852005.csv")

# check that both refer to the same training. only date should be different
for col in df1.columns:
    print(df1[col])
    print(df2[col])

# now make one joint linking info (we just write one of them)
df1.to_csv(datapath+"linked_ids_temp/linking_info_advisors_biology_christoph_degree0.csv", index=False)

# now load links 
df1 = pd.read_csv(datapath+"archive_linked_ids_temp/links_advisors_biology_christoph_degree0_20002022.csv")
df2 = pd.read_csv(datapath+"archive_linked_ids_temp/links_advisors_biology_christoph_degree0_19852005.csv")

df = pd.concat([df1, df2], axis=0, ignore_index=True)

# problem: overlapping years, same dissertation-advisor combination could be matched to two different authors (or duplicated)
# solution pick highest score -> sort by link score and keep the last observation within relationship_id
df_maxscore = df.sort_values("link_score").drop_duplicates("relationship_id", keep="last")

df_maxscore.to_csv(datapath+"linked_ids_temp/links_advisors_biology_christoph_degree0.csv", index=False)

os.remove(datapath+"linked_ids_temp/links_advisors_biology_christoph_degree0_20002022.csv")
os.remove(datapath+"linked_ids_temp/links_advisors_biology_christoph_degree0_19852005.csv")


os.remove(datapath+"linked_ids_temp/linking_info_advisors_biology_christoph_degree0_20002022.csv")
os.remove(datapath+"linked_ids_temp/linking_info_advisors_biology_christoph_degree0_19852005.csv")
