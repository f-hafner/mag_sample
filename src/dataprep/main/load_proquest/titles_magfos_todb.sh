#!/bin/bash

DB_PATH="/mnt/ssd/AcademicGraph/AcademicGraph.sqlite"
TSV_DIR="/mnt/ssd/titles_pq_fos"

# Create initial SQL to set up the table
sqlite3 "$DB_PATH" <<EOF
CREATE TABLE IF NOT EXISTS "pq_title_magfos" (
  "goid" INTEGER,
  "fieldrank" INTEGER,
  "FieldOfStudyId" INTEGER,
  "score" REAL
);
EOF

# Process each TSV file
for tsv_file in "$TSV_DIR"/*.txt; do
    echo "Processing $tsv_file"
    # Use grep to exclude lines containing "goid" and pipe directly to sqlite
    grep -v 'goid' "$tsv_file" | sqlite3 "$DB_PATH" ".mode tabs" ".import /dev/stdin pq_title_magfos"
done

# Create indexes after all data is imported
sqlite3 "$DB_PATH" <<EOF
CREATE UNIQUE INDEX IF NOT EXISTS idx_pq_title_magfos ON pq_title_magfos (goid ASC, FieldOfStudyId ASC);
CREATE INDEX IF NOT EXISTS idx_pq_title_magfos_fos ON pq_title_magfos (FieldOfStudyId ASC);
EOF