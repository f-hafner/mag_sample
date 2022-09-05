"""
Define comparator functions and fields for dedupe
"""


import re

# 1. Comparator functions

def same_institution_type(s1, s2): # using this requires that one labels pairs with "no" when one has the med string but not the other
    """
    teach the model to lower weights on pairs that do not have 
    similar institution names
    """
    institution_names = ["college", "university", "institute"] 
    for x in institution_names:
        if x in s1:
            if x in s2:
                return 1
        else:
            return 0

def equal_if_main_campus(s1, s2):
    if s1 + " main campus" == s2 or s2 + " main campus" == s1:
        return 1
    else:
        return 0

def equal_if_no_at(s1, s2):
    if re.sub(" at ", " ", s1) == s2 or re.sub(" at ", " ", s2) == s1:
        return 1
    else:
        return 0


def compare_city_names(s1, s2):
    "Check if the city in s2 appears in uni name s1"
    if s2 in s1:
        return 1
    else:
        return 0


# 1. Variable definitions for dedupe 

fields_mag = [
    {"field": "name",
        "variable name": "name", 
        "type": "String"},
    {"field": "location", 
        "variable name": "location", 
        "type": "LatLong"},
    {"type": "Interaction", 
        "interaction variables": ["name", "location"]},
    {"field": "name", 
        "variable name": "equal_if_main_campus", 
        "type": "Custom", 
        "comparator": equal_if_main_campus}, 
    {"field": "name", 
        "variable name": "equal_if_no_at", 
        "type": "Custom", "comparator": equal_if_no_at}, 
    {"type": "Interaction", 
        "interaction variables": ["equal_if_main_campus", "location"]},
    {"type": "Interaction", 
        "interaction variables": ["equal_if_no_at", "location"]}
] 


fields_pq = [
    {"field": "name", 
        "variable name": "name",
        "type": "String"},
    {"field": "stabbr", 
        "variable name": "state", 
        "type": "Exact", 
        "has missing": True},
    {"field": "name", 
        "variable name": "equal_if_main_campus", 
        "type": "Custom", 
        "comparator": equal_if_main_campus},
    {"field": "name", 
        "variable name": "equal_if_no_at", 
        "type": "Custom", 
        "comparator": equal_if_no_at},
    {"field": "city", 
        "variable name": "city_in_name",
        "type": "Custom", 
        "comparator": compare_city_names},
    {"type": "Interaction", 
        "interaction variables": ["city_in_name", "name"]},
    {"type": "Interaction", 
        "interaction variables": ["city_in_name", "state"]}
 ]