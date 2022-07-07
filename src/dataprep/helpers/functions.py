import time 
import re
from nltk.metrics.distance import jaro_winkler_similarity

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
    s = s.replace("[^\\w\\d]", " ", regex = True) # fixed to double escape: https://bugs.python.org/issue32912
    s = s.replace("\\s+", " ", regex = True) # ditto above
    s = s.str.strip()
    return s


def ntimes_in_string(s, x):
    "Count how often word x occurs in string s."
    return(sum([x in i for i in s.split(" ")]))

def all_tuple_or_list(l):
    "Check whether all elements in a list are tuples or lists."
    check = [isinstance(i, list) | isinstance(i, tuple) for i in l]
    if sum(check) != len(l):
        raise TypeError("Not all elements are a list or a tuple.")
    else:
        return True

def wordlist_is_unique(s, l):
    "Check whether each word of a list is unique in a string"
    n_words_in_l = [ntimes_in_string(s, i) for i in l]
    if max(n_words_in_l) > 1:
        raise ValueError("some words in string are duplicated.")
    else:
        return True


# ## Functions for linking 

def tupelize_links(links, iteration_id):
    """
    Return a tuple from the list `links`; add `iteration_id` as the last element of the tuple.
    """
    for i in links:
        id0, id1, score = i[0][0], i[0][1], i[1]
        yield id0, id1, score, iteration_id


def jw_comparator(field_1, field_2):
    """
    Return the jaro-winkler string similarity between `field 1` and `field_2`.
    """
    try:
        jw = jaro_winkler_similarity(field_1, field_2)
        return(jw)
    except:
        return(None)


def string_set_similarity(string, set):
    """
    Return the jaro-winkler string similarity of the one element in `set` 
    with the highest similarity to `string`.
    """
    try:
        similarities = (jaro_winkler_similarity(string, x) for x in set)
        return(max(similarities))
    except:
        return(None)

def max_set_similarity(set_1, set_2):
    """
    Return the jaro-winkler string similarity of most similar elements in `set_1` and `set_2` 
    """
    try:
        similarities = (string_set_similarity(x, set_2) for x in set_1)
        return(max(similarities))
    except:
        return(None)



def max_set_similarity_ignoreuni(set_1, set_2):
    """
    Return the jaro-winkler string similarity of most similar elements in `set_1` and `set_2`, 
    but ignore `university` in the strings in the set.
    """
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
    """
    Compare year of first publication with year of graduation.
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
    """
    Return indicator for graduation year pre-1986.
    """
    # year_firstpub is not used, but should stay there because dedupe compares always two fields with each other
    if year_graduation < 1986:
        return 1
    else:
        return 0


def drop_firstword(s, x):
    """
    Drop first word in string `s` if it is `x` 
    """
    ls = s.split(" ")
    if ls[0] == x:
        ls = ls[1:]
    out = " ".join(ls)
    return out

def prep_sql_query(query, params): 
        """
        Prepare a SQLite query from a statement and a dictionary 
            with conditions. The result can be passed as a query
            and parameters to sqlite3 or pandas.
            If multiple params are given, they are compounded into
            one list in the order they appear in the query.

        Parameters
        ----------
        query : A SQLite query from the database. Conditions 
            have to be marked by a unique string, which is
            referred to in `params`.

        params : A dictionary. The keys are strings that identify the 
            location in the query where the parameters should go. 
            The values is a list or a tuple of values
            that are to be kept.

        Returns
        ------
        A dictionary with two items: the `query` in qmark format, 
            and the `parameters` as a list.

        Example
        -------
        >>> prep_sql_query("select * from a where x in cond1", 
                {"cond1": [1, 2]}) 
        >>> # returns {"query": "select * from a where x in (?, )", "parameters": [1, 2]}
        """
        all_tuple_or_list(params.values())
        wordlist_is_unique(query, params.keys())

        value_dict = {k: ",".join(["?" for i in range(len(v))]) for k, v in params.items() }
        position_dict = {k: query.find(k) for k in params.keys()}
        
        for k in value_dict.keys():
            query = query.replace(k, f"({value_dict[k]})")
        
        value_order = sorted(position_dict)
        arguments = [list(params[k]) for k in value_order]
        arguments = sum(arguments, [])
        out = {"query": query, "parameters": arguments}
        return(out)


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

