#!/usr/bin/python
# -*- coding: utf-8 -*-

#import src.dataprep.helpers.comparator_functions as cf
#import src.dataprep.helpers.tfidf_settings as ts
#from nltk.metrics.distance import jaro_winkler_similarity
#from sklearn.feature_extraction.text import TfidfVectorizer

#import numpy 
#import pytest

# run with python3 -m pytest!


tuple_1 = (2006, "biology")
tuple_2 = (2008, "ecology")
tuple_3 = (1998, "chemistry")
tuple_4 = (2010, "sociology")
tuple_5 = (2008, "biology")
set1 = {tuple_1, tuple_2} #from mag
set2 = {tuple_3, tuple_4, tuple_5} #from  nsf


def field_comparator(set1, set2):
    """
    return 1 if the fields match within a time span of -5, +7 years
    """
    for tup1 in set1:
        for tup2 in set2:
            year1, field1 = tup1
            year2, field2 = tup2

            if field1 == field2:  # Compare string parts (field)
                if year2 - 5 <= year1 <= year2 + 7:  # Compare numeric parts (year)
                    return 1

    return 0



result = field_comparator(set1, set2)
print(result)  



