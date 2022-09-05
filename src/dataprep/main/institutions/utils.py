
import pandas as pd 
import numpy as np
import re 

dedupe_datapath = "/mnt/ssd/DedupeFiles/links_to_cng/"


def extract_parenth(s):
    "Extract string in parenthesis"
    start = s.find("(")
    end = s.find(")")
    if start >= 0 & end >= 0:
        return(s[start+1:end])
    else:
        return None 

def links_to_row(l):
    "convert linking output from dedupe to an np array"
    ids = l[0]
    score = l[1]
    out = [ids[0], ids[1], score]
    out = np.array(out)
    return(out)

