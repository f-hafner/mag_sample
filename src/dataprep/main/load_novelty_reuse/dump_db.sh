#!/bin/sh

# Path to the SQLite Database
DB_PATH="/mnt/ssd/AcademicGraph/AcademicGraph.sqlite"

# Relative or tilde path to the CSV file
CSV_PATH="~/novelty/papers_textual_metrics.csv"

# Expand the tilde to an absolute path
EXPANDED_CSV_PATH=$(eval echo $CSV_PATH)

# SQLite commands
sqlite3 $DB_PATH <<EOF
DROP TABLE IF EXISTS novelty_reuse;
EOF

sed 1d "$EXPANDED_CSV_PATH" > temp.csv 

sqlite3 $DB_PATH <<EOF
CREATE TABLE IF NOT EXISTS novelty_reuse (
    PaperID INTEGER,
    new_word INTEGER,
    new_word_reuse INTEGER,
    new_bigram INTEGER,
    new_bigram_reuse INTEGER,
    new_trigram INTEGER,
    new_trigram_reuse INTEGER,
    new_word_comb INTEGER,
    new_word_comb_reuse INTEGER,
    cosine_max REAL,
    cosine_avg REAL,
    n_words INTEGER,
    n_bigrams INTEGER,
    n_trigrams INTEGER
);
.mode csv
.import temp.csv novelty_reuse
EOF
rm temp.csv

# Create an index on the PaperID column
sqlite3 $DB_PATH "CREATE UNIQUE INDEX IF NOT EXISTS idx_nr_PaperId ON novelty_reuse(PaperID);"


# Check that number of rows matches 
echo "nrows expected: 72,245,396"
sqlite3 $DB_PATH "SELECT COUNT(*) FROM novelty_reuse;"
