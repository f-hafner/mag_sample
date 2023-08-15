#!/usr/bin/python
# -*- coding: utf-8 -*-

import src.dataprep.helpers.comparator_functions as cf
import src.dataprep.helpers.tfidf_settings as ts
from nltk.metrics.distance import jaro_winkler_similarity
from sklearn.feature_extraction.text import TfidfVectorizer

import numpy 
import pytest

# run with python3 -m pytest!

s1 = "university of chicago"
s2 = "harvard university"
n1 = 5
n2 = 10

similar_tuple_1 = (1994, "university of chicago")
similar_tuple_2 = (1995, "university of chicago")
different_tuple_1 = (2006, "new york university")
different_tuple_2 = (2010, "harvard university")
set1 = (similar_tuple_1, different_tuple_1)
set2 = (similar_tuple_2, different_tuple_2)


def test_string_comparator():
    assert cf.string_comparator(s1, s2) == 1 - jaro_winkler_similarity(s1, s2), \
        "incorrect string distance"

def test_number_comparator():
    assert cf.number_comparator(n1, n2) == abs(numpy.log10(n1) - numpy.log10(n2)), \
        "incorrect numerical distance"

def test_compare_values():
    assert cf.compare_values(n1, n2) == abs(numpy.log10(n1) - numpy.log10(n2)), \
        "fails on numbers"
    assert cf.compare_values(s1, s2) == 1 - jaro_winkler_similarity(s1, s2), \
        "fails on strings"
    with pytest.raises(TypeError, match = "need to be of the same type"):
        cf.compare_values(n1, s1)

def test_tuple_distance():
    stringtup1 = ("university of chicago", "massachusetts institute of technology")
    stringtup2 = ("university of schicago", "new york university")
    distance = cf.tuple_distance(stringtup1, stringtup2)
    target = 1 - jaro_winkler_similarity("chicago", "schicago") # remember that university|university of is dropped
    assert distance == target, "does not return smallest distance"


def test_set_of_tuples_distance_string():
    distance = cf.set_of_tuples_distance_string(set1, set2)
    target = 1 - jaro_winkler_similarity(similar_tuple_1[1], similar_tuple_2[1])
    assert distance == target, "does not return smallest string distance"


def test_set_of_tuples_distance_number():
    distance = cf.set_of_tuples_distance_number(set1, set2)
    target = numpy.log10(similar_tuple_1[0]) - numpy.log10(similar_tuple_2[0])
    assert distance == abs(target), "does not return smallest num distance"


def test_set_of_tuples_distance_overall():
    stringdist = 1 - jaro_winkler_similarity(similar_tuple_1[1], similar_tuple_2[1])
    numdist = abs(numpy.log10(similar_tuple_1[0]) - numpy.log10(numpy.log10(similar_tuple_2[0])))
    target = stringdist * numdist
    distance = cf.set_of_tuples_distance_overall(set1, set2)
    assert distance == target, "does not return smallest overall distance"

def test_compare_range_from_tuple():
    short = (1990, )
    long = (1995, 2005)
    assert cf.compare_range_from_tuple(short, long) == 0, "wrong output"
    assert cf.compare_range_from_tuple(short, long) == cf.compare_range_from_tuple(long, short), \
        "arguments are not interchangeable"
    short = (1999, )
    long = (1995, 2005)
    assert cf.compare_range_from_tuple(short, long) == 1, "wrong output"
    with pytest.raises(ValueError, match="wrong length"):
        cf.compare_range_from_tuple(long, long)
    
    notuple = 3
    with pytest.raises(TypeError, match="need to be tuples"):
        cf.compare_range_from_tuple(notuple, long)

def test_compare_startrange_from_tuple():
    short = (1990, )
    long = (1995, 2005)
    assert cf.compare_startrange_from_tuple(short, long) == -5, "wrong output"

    short = (1999, )
    long = (1995, 2005)
    assert cf.compare_startrange_from_tuple(short, long) == 0, "wrong output"    

    notuple = 3
    with pytest.raises(TypeError, match="need to be tuples"):
        cf.compare_startrange_from_tuple(notuple, long)

def test_compare_endrange_from_tuple():
    short = (1990, )
    long = (1995, 2005)
    assert cf.compare_endrange_from_tuple(short, long) == 0, "wrong output"

    short = (2006, )
    long = (1995, 2005)
    assert cf.compare_endrange_from_tuple(short, long) == 1, "wrong output"    

    notuple = 3
    with pytest.raises(TypeError, match="need to be tuples"):
        cf.compare_endrange_from_tuple(notuple, long)

def test_compare_range_from_tuple_tempfix(capfd):
    long1 = (1995, 2005)
    long2 = (2000, 2010)
    assert cf.compare_range_from_tuple_tempfix(long1, long2) is None 
    out, err = capfd.readouterr()
    assert "error occurred" in out



thesis_title = ["a dissertation about my favorized topics"]
paper_titles = ["a paper about an unrelated topic",
                "a published paper about my favorite topic based on the dissertations chapter"]
long_set_of_titles = [
    "a published paper about my favorite topic based on the dissertations chapter",
    "something about cats",
    "something about dogs",
    "something about mice"
]

def test_text_comparator():
    sim = cf.text_comparator(thesis_title, paper_titles)
    Vectorizer = TfidfVectorizer(
        analyzer=cf.stemmed_words,
        stop_words=ts.stop_words,
        ngram_range=ts.ngram_range
    )
    tfidf = Vectorizer.fit_transform(thesis_title + paper_titles)
    target = tfidf * tfidf.T
    target = target.toarray()[0,2]
    assert sim == target, "does not return the right text similarity"

    sim = cf.text_comparator(paper_titles, paper_titles)
    assert sim == pytest.approx(1), "wrongly handles len(a) > 1"

    sim = cf.text_comparator(paper_titles, long_set_of_titles)
    assert sim == pytest.approx(1), "wrongly handles inputs of different length > 1"

    # with pytest.raises(ValueError, match="empty"):
    #     cf.text_comparator([], paper_titles)

def test_keyword_comparator():
    set1 = ("organic chemistry", "molecular biology")
    set2 = ("labor economics", "organic chemistry")
    set3 = ("inorganic chemistry", "computer science")
    assert cf.keyword_comparator(set1, set2) == 1
    assert cf.keyword_comparator(set1, set3) == 0