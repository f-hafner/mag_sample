# Some functions for comparing records between data sets 

from math import nan
import re
from operator import mul 
from functools import reduce, wraps 

from nltk.metrics.distance import jaro_winkler_similarity
from nltk.stem import SnowballStemmer
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
import numpy 

from .functions import is_numeric, list_from_tuples
from .decorators import decorator_with_args
from .tfidf_settings import stop_words, ngram_range

# 1. Basic comparators

def string_comparator(string1, string2, ignore_substr=None):
    """
    Compare string1 and string2; if required first delete 
    `ignore_substr` from both.
    """
    if ignore_substr is not None:
        string1 = re.sub(ignore_substr, "", string1).strip()
        string2 = re.sub(ignore_substr, "", string2).strip()
    try:
        jw = jaro_winkler_similarity(string1, string2)
        return 1 - jw
    except:
        return None


def number_comparator(x, y):
    "Compare two numbers."
    dist = abs(numpy.log10(x) - numpy.log10(y))
    return dist 


def compare_values(a, b, ignore_substr=None):
    "Compare values depending on their type."
    if isinstance(a, str) & isinstance(b, str):
        out = string_comparator(a, b, ignore_substr=ignore_substr)
    elif is_numeric(a) & is_numeric(b):
        out = number_comparator(a, b)
    else:
        raise TypeError("Variables need to be of the same type.")
    return out


def compare_two_single_tuples(fnc):
    """
    Compare entries in two tuples.
    Return the distance between the two least distant.
    The distance function is passed as argument.
    """
    @wraps(fnc)
    def compare(tuple1, tuple2):
        out = 10e7 # set to some arbitary, non-infinite number
        for i in tuple1:
            for j in tuple2:
                #print(f"Comparing {i} and {j} yields {fnc(i, j)}")
                if fnc(i, j) < out:
                    out = fnc(i, j)
        return out
    return compare


@compare_two_single_tuples
def tuple_distance(a, b): 
    "Calculate the smallest distance between the values in two tuples."
    return compare_values(a, b, ignore_substr="university of|university")


def keyword_comparator(a, b):
    "Return 1 if at least one keyword in a is also in b, else 0."
    count = sum([1 for x in a if x in b])
    if count > 0:
        return 1
    else:
        return 0


# 2. Compare sets of tuples and return minimum distance

# ## Positions in the tuples sourced from MAG/NSF--hardcoded!
string_position = 1
number_position = 0
use_both_positions = 2

# ## Decorators
@decorator_with_args
def compare_sets_of_tuples(fnc, position):
    """
    Compare two sets of tuples with two entries each.
    Either compare specific entries in the tuples, or compare the 
    product of the distance between both entries.
    """
    @wraps(fnc)
    def compare(set1, set2):
        out = 10e7 # set to some arbitary, non-infinite number
        for tuple1 in set1:
            for tuple2 in set2:
                if position == 2:
                    dist = reduce(mul, [fnc((tuple1[i], ), (tuple2[i], )) for i in range(2) ]) 
                else:
                    dist = fnc((tuple1[position], ), (tuple2[position], ))
                
                if dist < out:
                    out = dist
        return out
    return compare


# ## Define functions
# NOTE: The function definition is a bit counter-intuitive. 
    # The main function for usage are set_of_tuples_distance_["string", "number", "overall"].
    # But their input arguments `a` and `b` are arguments to comparing two single values.
@compare_sets_of_tuples(string_position)
@compare_two_single_tuples
def set_of_tuples_distance_string(a, b): 
    """
    For two sets of tuples that both contain a string and a numeric, 
    return the distance of the two closest strings. 
    
    Parameters
    ----------
    a, b : Values in the two different tuples. The values are extracted 
        with the argument in `compare_sets_of_tuples`
    """
    return compare_values(a, b, ignore_substr="university of|university")
    

@compare_sets_of_tuples(number_position)
@compare_two_single_tuples
def set_of_tuples_distance_number(a, b):
    """
    For two sets of tuples that both contain a string and a numeric, 
    return the distance of the two closest numbers.

    Parameters
    ----------
    a, b : Values in the two different tuples. The values are extracted 
        with the argument in `compare_sets_of_tuples`
    """
    return compare_values(a, b)
    

@compare_sets_of_tuples(use_both_positions)
@compare_two_single_tuples
def set_of_tuples_distance_overall(a, b):
    """
    For two sets of tuples that both contain a string and a numeric, 
    return the overall distance of the most similar tuple.

    Parameters
    ----------
    a, b : Values in the two different tuples. The values are extracted 
        with the argument in `compare_sets_of_tuples`
    """
    return compare_values(a, b)


# 3. Check whether a singleton is in a year range defined by another tuple
def compare_range_from_tuple(a, b):
    """
    Check whether the one-element tuple 
    is in the range formed by the two-element tuple

    Parameters:
    ----------
    a, b : tuples of numbers. One needs to be of length 1, the other of length 2.
    """
    margin = 4 # extend the range by +/- this 
    # print("comparing ranges..", flush=True)
    # print(f"--a is {a}", flush=True)
    # print(f"--b is {b}", flush=True)
    if isinstance(a, tuple) and isinstance(b, tuple): 
        if len(a) == 1:
            value = a
            range_bounds = b
        elif len(b) == 1:
            value = b
            range_bounds = a
        else:
            raise ValueError("Tuples are of wrong length.")
                # Dedupe sometimes passes two tuples of length 2 
                # for no reason (and they seem often to be the same).
                # As a temp fix: Uncomment the prints above and run the script to see it.
            # return None
    else:
        raise TypeError("a, b need to be tuples.")
    
    value = value[0]
    if (value >= (min(range_bounds) - margin) 
        and value <= (max(range_bounds) + margin)):
        return 1
    else:
        return 0


def compare_range_from_tuple_tempfix(a, b):
    """
    A temp fix to make labelling and training work.
    It seems that dedupe sometimes compares one record with itself,
    and this violates our assumptions on the structure of the data.
    """
    try:
        compare_range_from_tuple(a, b)
    except:
        print(
            f"An error occurred when calling compare_range_from_tuple({a}, {b}). "
            "I cannot print the type, but most likely a TypeError or ValueError."
            , flush=True
            )
            # printing the type gives "Segmentation fault (core dumped)". 
            # https://stackoverflow.com/questions/13654449/error-segmentation-fault-core-dumped
        return None

# NOTE: it is well possible that dedupe compares also records *within* data sets when doing a 
    # record-linking task--after all, it needs to block them eventually.

# 4. Compare paper titles

def year_title_comparator(x, y):
    """
    Compare tuples of year-title combinations in x and y.

    Parameters
    ----------
    x: A tuple of (year, title) combination
    y: A tuple of one or mor tuples of (year, title) combinations
    """
    # extract the strings from the year-title pairs
    x = list_from_tuples(x)
    y = list_from_tuples(y)
    return text_comparator(x, y)
    

def text_comparator(a, b):
    """
    Compare similarity of two tuples of text.

    Parameters
    ----------
    a: list of texts
    b: list of texts

    The function returns the maximum similarity of the 
    first text in a to any of the texts in b.
    """
    # TODO: how does it deal with numbers? ie chemical molecules indexed by numbers?
    # TODO: check the tokenizer -- does it use the same stopwords by default? 
    corpus = a + b

    Vectorizer = TfidfVectorizer(
        analyzer=stemmed_words,
        stop_words=stop_words,
        ngram_range=ngram_range
    )

    tfidf = Vectorizer.fit_transform(corpus)
    pairwise_similarity = tfidf * tfidf.T
    # we want the upper right matrix that compares the distance between elements in a and elements in b
        # include rows 0 to len(a)
        # include columns len(a) to len(a) + len(b)
    similarity = pairwise_similarity.toarray()[0:len(a), len(a):]
    return numpy.max(similarity)


def stemmed_words(doc):
    "Stem words with same settings as Tfidf vectorizer."
    stemmer = SnowballStemmer("english")
    Vectorizer = CountVectorizer(
        stop_words=stop_words,
        ngram_range=ngram_range
    )
    analyzer = Vectorizer.build_analyzer() 
    return (stemmer.stem(w) for w in analyzer(doc))

