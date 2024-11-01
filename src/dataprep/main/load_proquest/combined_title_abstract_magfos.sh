 #!/bin/bash

 DB_PATH="/mnt/ssd/AcademicGraph/AcademicGraph.sqlite"

sqlite3 "$DB_PATH" <<EOF
 DROP TABLE IF EXISTS pq_magfos_abs_title;
 CREATE TABLE pq_magfos_abs_title (
   "goid" INTEGER,
   "fieldrank" INTEGER,
   "FieldOfStudyId" INTEGER,
   "score" REAL
 );

 -- Use abstract-based predictions when enough words
INSERT INTO pq_magfos_abs_title (goid, fieldrank, FieldOfStudyId, score)
    SELECT pm.goid, pm.fieldrank, pm.FieldOfStudyId, pm.score
    FROM pq_attributes pa
    JOIN pq_magfos pm ON pa.goid = pm.goid
    WHERE pa.abswordcount >= 20;


-- Use title-based predictions otherise
INSERT INTO pq_magfos_abs_title (goid, fieldrank, FieldOfStudyId, score)
    SELECT pt.goid, pt.fieldrank, pt.FieldOfStudyId, pt.score
    FROM pq_attributes pa
    LEFT JOIN (SELECT DISTINCT goid from pq_magfos) pm ON pa.goid = pm.goid
    JOIN pq_title_magfos pt ON pa.goid = pt.goid
    WHERE pa.abswordcount < 20
       OR pm.goid IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_pq_magfos_abs_title ON pq_magfos_abs_title (goid ASC, FieldOfStudyId ASC);
CREATE INDEX IF NOT EXISTS idx_pq_magfos_abs_title_fos ON pq_magfos_abs_title (FieldOfStudyId ASC);
EOF