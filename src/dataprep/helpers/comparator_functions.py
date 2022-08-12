# Some functions for comparing records between data sets 

import re
from operator import mul 
from functools import reduce, wraps 

from nltk.metrics.distance import jaro_winkler_similarity
import numpy

from .functions import is_numeric
from .decorators import decorator_with_args

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





