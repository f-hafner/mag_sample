import time 

def print_elapsed_time(start_time):
    print(f"Time elapsed: {(time.time()-start_time)/60} minutes \n", flush = True)


def analyze_db(con):
    """
    Run `analyze` commands on `con` according to sqlite recommendations.
    """
    print("Running ANALYZE... \n")
    cursor = con.cursor()
    cursor.execute("PRAGMA analysis_limit = 1000")
    cursor.execute("PRAGMA optimize")
    cursor.close()



def normalize_string(s, replace_hyphen = ""): 
    """
    Normalize the character strings in a pd.Series
    """
    letters_to_replace = {
        "ä": "a",
        "ü": "u",
        "ö": "o",
        "í": "i",
        "ì": "i",
        "ñ": "n",
        "à": "a",
        "á": "a",
        "é": "e",
        "è": "e"
    }
    s = s.str.lower() 
    s = s.replace("\\. ", " ", regex = True)
    s = s.replace("-", replace_hyphen, regex = True)
    s = s.replace("\\.", "", regex = True) # for dots followed by non-whitespace
    s = s.replace(letters_to_replace, regex = True)
    # this is mostly for titles, but I guess it does not hurt for the names either
    s = s.replace("[^\w\d]", " ", regex = True)
    s = s.replace("\s+", " ", regex = True)
    s = s.str.strip()
    return s



def drop_firstword(s, x):
    """
    Drop first word in string `s` if it is `x` 
    """
    ls = s.split(" ")
    if ls[0] == x:
        ls = ls[1:]
    out = " ".join(ls)
    return out


def tupelize_links(links, iteration_id):
    """
    Return a tuple from the list `links`; add `iteration_id` as the last element of the tuple.
    """
    for i in links:
        id0, id1, score = i[0][0], i[0][1], i[1]
        yield id0, id1, score, iteration_id


def is_numeric(a):
    "Check if a is numeric."
    return isinstance(a, int) | isinstance(a, float)
    


# ### Some functions for reading sqlite data into dict

def dict_factory(cursor, row):
    """
    Return row as dict. Keys are column names, values are row entries.
    """
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def custom_enumerate(s, idx):
    """
    Yield tuple, with the index `idx` of the record instead the 0-based index of `enumerate`.
    """
    for elem in s:
        yield elem[idx], elem

