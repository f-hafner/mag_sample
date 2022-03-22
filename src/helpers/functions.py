import time 
import re
from nltk.metrics.distance import jaro_winkler_similarity

def print_elapsed_time(start_time):
    print(f"Time elapsed: {(time.time()-start_time)/60} minutes \n", flush = True)


# Analyze and optimize the sqlite db 
def analyze_db(con):
    print("Running ANALYZE... \n")
    cursor = con.cursor()
    cursor.execute("PRAGMA analysis_limit = 1000")
    cursor.execute("PRAGMA optimize")
    cursor.close()



# ## Function to normalize names 
def normalize_string(s, replace_hyphen = ""): 
    """Normalize the character strings in a pd.Series"""
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



# ## Functions for linking 
# def tupelize_links(links, created_date, start = args.startyear, end = args.endyear):
#     for i in links:
#         id0, id1, score = i[0][0], i[0][1], i[1]
#         yield id0, id1, score, start, end, created_date
def tupelize_links(links, iteration_id):
    for i in links:
        id0, id1, score = i[0][0], i[0][1], i[1]
        yield id0, id1, score, iteration_id


def jw_comparator(field_1, field_2):
    try:
        jw = jaro_winkler_similarity(field_1, field_2)
        return(jw)
    except:
        return(None)


def string_set_similarity(string, set):
    """Return the string similarity of the one element in `set` with the highest similarity to `string`"""
    try:
        similarities = (jaro_winkler_similarity(string, x) for x in set)
        return(max(similarities))
    except:
        return(None)

def max_set_similarity(set_1, set_2):
    """Return the string similarity of most similar elements in `set_1` and `set_2` """
    try:
        similarities = (string_set_similarity(x, set_2) for x in set_1)
        return(max(similarities))
    except:
        return(None)




def max_set_similarity_ignoreuni(set_1, set_2):
    """Ignore `university` in the strings in the set"""
    def drop_string_from_set(set, string):
        set = [re.sub(string, "", x).strip() for x in set]
        set = [" ".join(x.split()) for x in set]
        return(set)  
    try:
        ignore_strings = "university of|university"
        set_1 = drop_string_from_set(set_1, ignore_strings)
        set_2 = drop_string_from_set(set_2, ignore_strings)
        return(max_set_similarity(set_1, set_2))
    except:
        return(None)



def name_comparator(name_1, name_2):
    if name_1 == name_2:
        return(1)
    else:
        return(0)

def squared_diff(x_1, x_2):
    sq = (x_1 - x_2)^2
    return(sq)


def grad_pub_year_comparator(year_firstpub, year_graduation):
    """Compare year of first publication with year of graduation.
    Because the function is not symmetric, it's important that the data are in the right order, eg, first the data
        on publication, then on graduation.
    This assumes that graduates do not publish yr_threshold years before PhD graduation, eg. as an RA or in a
        previous job (Fed, ...).
    """
    yr_diff = year_firstpub - year_graduation
    yr_threshold = 8
    if yr_diff <= -yr_threshold:
        out = -10
    elif yr_diff >= yr_threshold:
        out = 0
    else:
        out = 10 * (1 - abs(yr_diff) / yr_threshold)
    return(out)

def year_dummy_noadvisor(year_firstpub, year_graduation):
    """Return indicator for graduation year pre-1980. """
    # year_firstpub is not used, but should stay there because dedupe compares always two fields with each other
    if year_graduation < 1986:
        return 1
    else:
        return 0


def drop_firstword(s, x):
    """Drop first word in string `s` if it is `x` """
    ls = s.split(" ")
    if ls[0] == x:
        ls = ls[1:]
    out = " ".join(ls)
    return out


# ### Some functions for reading sqlite data into dict

def dict_factory(cursor, row):
    """Return row as dict. Keys are column names, values = row entries."""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def custom_enumerate(s, idx):
    """Return tuple, with the index `idx` of the record instead the 0-based index of `enumerate`"""
    for elem in s:
        yield elem[idx], elem

