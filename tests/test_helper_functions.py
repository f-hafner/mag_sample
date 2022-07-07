#!/usr/bin/python
# -*- coding: utf-8 -*-

from src.dataprep.helpers.functions import prep_sql_query
from src.dataprep.helpers.functions import all_tuple_or_list, wordlist_is_unique
import pytest 

# run with python3 -m pytest!


def test_prep_sql_query():
    q = "select * from a where b in cond1 and c in cond2"
    v = {"cond1": (1, 2), "cond2": [3]}
    output = {"query": "select * from a where b in (?,?) and c in (?)",
                "parameters": [1, 2, 3]}
    assert prep_sql_query(q, v) == output, "incorrect output from prep_sql_query"
    # change order -> same output
    v = {"cond2": [3], "cond1": (1, 2)}
    assert prep_sql_query(q, v) == output, "prep_sql_query messes up order"


def test_all_tuple_or_list():
    assert all_tuple_or_list([[1, 2], [4, 5]]), "list of list not detected"

    with pytest.raises(TypeError, match = "a list or a tuple"):
        all_tuple_or_list([3, [4, 5]])


def test_wordlist_is_unique():
    assert wordlist_is_unique("blah word1 word2", ["word1", "word2"]), "unique words not detected"

    with pytest.raises(ValueError, match = "duplicated"):
        wordlist_is_unique("blah word1 word1", ["word1"])




