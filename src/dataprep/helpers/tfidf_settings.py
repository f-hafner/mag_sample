
"""
settings for tfidf: Inputs for Tfidf Vectorizer

- A conservative list of stopwords for 
    NLP on titles of scientific papers 
    and dissertations
- ngram_range 
"""

stop_words = [
    "and"
    , "for"
    , "to"
    , "of"
    , "from"
    , "a"
    , "an"
    , "in"
    , "the"
    , "by"
    , "or"
    , "other"
    , "too"
    , "very"
    , "really"
    , "this"
    , "that"
    , "it"
]

# (because), little, few, big, large, small

ngram_range = (1,2)