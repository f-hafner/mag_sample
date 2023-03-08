import pandas as pd
import numpy as np 

def cosine_similarity_on_df(df):
    "Final step for calculating cosine similarity in a dataframe"
    df["sim"] = (
        df["AB"] / (np.sqrt(df["AA"] + 1e-7) * np.sqrt(df["BB"] + 1e-7))
    ) # 1e-7: small constant to avoid dividing by 0
    return df 

def fill_nas(df, varlist, fill=0):
    for v in varlist:
        df[v] = np.where(df[v].isna(), fill, df[v])
    return df 


def split_year_pre_post(df, ref_year):
    df["period"] = np.where(df["Year"] <= ref_year, "pre_phd", "post_phd")
    return df 
    
def unique(sequence):
    "Helper to keep unique elements in a list. " # https://stackoverflow.com/questions/9792664/converting-a-list-to-a-set-changes-element-order
    seen = set()
    return [x for x in sequence if not (x in seen or seen.add(x))]

