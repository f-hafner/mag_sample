
import os
import time
import logging
import optparse
import locale
import numpy as np
import multiprocessing as multproc
import sys
import pdb
import argparse
import pandas as pd
from datetime import datetime

import sqlite3 as sqlite

import dedupe
import dedupe.backport
from collections import OrderedDict

from helpers.variables import db_file, datapath
from helpers.functions import analyze_db, tupelize_links, max_set_similarity, dict_factory, custom_enumerate, name_comparator, year_dummy_noadvisor, max_set_similarity_ignoreuni

start_time = time.time()

class Range(object):
    def __init__(self, start, end):
        self.start = start
        self.end = end    
    def __eq__(self, other):
        return self.start <= other <= self.end


# ## Some settings
pd.set_option('display.max.columns', None)
path_dedupe_files = datapath + "DedupeFiles/"
share_blockedpairs_training = 0.66 # fraction of similar pairs as opposed to random pairs 

# register [adapter for numpy.int64](https://stackoverflow.com/questions/38753737/inserting-numpy-integer-types-into-sqlite-with-python3)
    # Since sqlite stores values [depending on the data type](https://www.sqlite.org/datatype3.html),
    # I think a float64 will become REAL of 8 byte
sqlite.register_adapter(np.int64, lambda val: int(val))
sqlite.register_adapter(np.int32, lambda val: int(val))
sqlite.register_adapter(np.float64, lambda val: float(val))
sqlite.register_adapter(np.float32, lambda val: float(val))

# ## Arguments
parser = argparse.ArgumentParser(description = 'Inputs for linking')
parser.add_argument('--test', dest='testing', action='store_true')
parser.add_argument('--no-test', dest='testing', action='store_false')
parser.add_argument("-v", "--verbose", dest = "verbose", action = "count",
                    help = "Increase verbosity (specify multiple times for more)")
parser.add_argument("--field", 
                    type = str,
                    nargs = "*",
                    default = "economics",
                    help = "Major field of study") 
parser.add_argument("--train_name", 
                    type = str,
                    default = "",
                    help = "add train_name to train and settings file name") 
parser.add_argument("--start", dest = "startyear", type = int, 
                    default = 1950, help = "Start year for records")
parser.add_argument("--end", dest = "endyear", type = int, 
                    default = 2015, help = "End year for records")
parser.add_argument("--mergemode", dest = "mergemode", type = str, 
                    default = "1:1", help = "m:1 or 1:1")
parser.add_argument("--recall", dest = "recall", type = float,
                    default = 0.9, choices = [Range(0.0, 1.0)],
                    help = "Higher recall recovers more links labelled as true, but lowers precision (more false positives)")
parser.add_argument("--institution", type=str, help = "use institution for learning?") 
parser.add_argument("--fieldofstudy_cat", type=str, help = "use fieldofstudy as categorical variable for learning?") 
parser.add_argument("--fieldofstudy_str", type=str, help = "use fieldofstudy as str for learning?") 
parser.add_argument("--keywords", type=str, help = "use keywords for learning?") 
parser.add_argument("--retrain", type = str, default = "True", help = "force retrain!?")   
parser.add_argument("--linking_type", type = str, default = "graduates",
                    help = "Are we linking graduates or advisors?", choices = {"graduates", "advisors", "grants"}) 
        
parser.set_defaults(testing="True")
parser.set_defaults(institution="False")
parser.set_defaults(fieldofstudy_cat="False")
parser.set_defaults(fieldofstudy_str="False")
parser.set_defaults(keywords="False")

args = parser.parse_args()
print(args, flush=True)

if type(args.field)==list: 
    field = " ".join(args.field) # not neded?? why
else: 
    field = args.field
    
field = field.strip()

# ### Define additional locals
pq_entity_id = "goid" # defines the link to proquest
tbl_linking_info = "linking_info"
tbl_linked_ids = "linked_ids"
# the order and names of columns for the linked identifiers
column_order_links = f"""AuthorId INT
                    , {pq_entity_id} INT """

if args.linking_type == "advisors":
    pq_entity_id = "relationship_id"
    tbl_linking_info = "linking_info_advisors"
    tbl_linked_ids = "linked_ids_advisors"
    path_dedupe_files = path_dedupe_files + "advisors/"
    column_order_links = f"""{pq_entity_id} TEXT
                        , AuthorId INT"""
    if not os.path.isdir(path_dedupe_files):
        os.mkdir(path_dedupe_files)
elif args.linking_type == "grants":
    nsf_entity_id = "GrantID"
    tbl_linking_info = "linking_info_grants"
    tbl_linked_ids = "linked_ids_grants"
    path_dedupe_files = path_dedupe_files + "grants/"
    column_order_links = f"""{nsf_entity_id} TEXT
                        , AuthorId INT"""
    if not os.path.isdir(path_dedupe_files):
        os.mkdir(path_dedupe_files)

# ### check the field here -- choices option in parser does not work straightforwardly with whitespaces
all_fields = ["history", "geology", "economics", "geography", "chemistry",
                    "philosophy", "sociology", "materials science", "mathematics",
                    "biology", "computer science", "political science",
                    "engineering", "psychology", "environmental science",
                    "business", "physics", "medicine", "art"]
possible_fields = all_fields + ["all"]

if field not in possible_fields:
    print(f"field needs to be in {possible_fields}, exiting")
    exit()

# convert to list for use below
field_to_store = field
if field == "all":
    field = all_fields
else:
    field = [field]

    
log_level = logging.WARNING
if args.verbose:
    if args.verbose == 1:
        log_level = logging.INFO
    elif args.verbose >= 2:
        log_level = logging.DEBUG

logging.getLogger().setLevel(log_level)

if args.startyear >= args.endyear:
    sys.exit("--start argument should be smaller than --end argument")

# ## number of cores
n_cores = int(multproc.cpu_count() / 2)
print('Have max %s cores available' % (n_cores), flush=True)

print(f"Testing is {args.testing} \n", flush=True)

if args.testing:
    n_cores = n_cores - 1

# ## Connections
read_con = sqlite.connect(database = db_file, isolation_level= None)
write_con = sqlite.connect(database = db_file, isolation_level= None)
read_dict_con = sqlite.connect(database = db_file, isolation_level = None)
    # last one is for reading in the data into dict for the training step

for c in [read_con, write_con, read_dict_con]:
    c.execute("PRAGMA journal_mode=WAL;")


read_dict_con.row_factory = dict_factory

# ### Get the id of the current field of study 
insert_field_questionmarks = ",".join(["?" for i in range(len(field))])

id_field = read_con.execute(
    f"""SELECT FieldOfStudyId 
    FROM FieldsOfStudy 
    WHERE Level = 0 
        AND NormalizedName IN ({insert_field_questionmarks})
    """, 
    tuple(field)
    )
id_field = [f[0] for f in id_field.fetchall()]

# SQL STATEMENTS FOR EXTRACTS
where_stmt = f"WHERE year >= {args.startyear} and year <= {args.endyear} AND length(firstname) > 1"
where_stmt_mag = f"WHERE length(firstname) > 1 AND year >= {args.startyear} - 5 AND year <= {args.endyear} + 5" # changed this to incorporate more people

if args.linking_type == "graduates":
    query_proquest = f"""
    SELECT goid
            , year
            , firstname 
            , lastname
            , CASE TRIM(SUBSTR(middle_lastname, 1, l_fullname-l_firstname-l_lastname - 1)) 
                WHEN 
                    "" THEN NULL 
                    ELSE TRIM(SUBSTR(middle_lastname, 1, l_fullname-l_firstname-l_lastname - 1)) 
                END AS middlename
            , fieldofstudy
            , keywords
            , institution
            , advisors AS coauthors
    FROM (
        SELECT goid
            , degree_year AS year 
            , fullname 
            , SUBSTR(TRIM(fullname),1,instr(trim(fullname)||' ',' ')-1) AS firstname
            , REPLACE(fullname, RTRIM(fullname, REPLACE(fullname, " ", "")), "") AS lastname 
            , TRIM(SUBSTR(fullname, length(SUBSTR(TRIM(fullname),1,instr(trim(fullname)||' ',' ')-1)) + 1)) AS middle_lastname 
            , length(fullname) AS l_fullname 
            , length(SUBSTR(TRIM(fullname),1,instr(trim(fullname)||' ',' ')-1) ) AS l_firstname
            , length(REPLACE(fullname, RTRIM(fullname, REPLACE(fullname, " ", "")), "")) AS l_lastname
            , fieldname AS fieldofstudy
            , university_id
        FROM pq_authors 
        INNER JOIN (
            SELECT goid, fieldname 
            FROM pq_fields_mag
            WHERE mag_field0 IN ({insert_field_questionmarks})
        ) USING (goid)
    )
    -- ## NOTE: use left join here as not all graduates have advisor (particularly pre-1980) and possibly also keywords
    LEFT JOIN pq_keywords USING(goid) 
    INNER JOIN (
        SELECT university_id, normalizedname as institution
        FROM pq_unis
        WHERE location like "%United States%"
    ) USING(university_id)
    LEFT JOIN pq_all_advisors USING(goid)
    {where_stmt}
    """

    query_mag = f"""
    SELECT f.AuthorId
        , f.year
        , f.firstname
        , f.lastname
        , CASE TRIM(SUBSTR(f.middle_lastname, 1, f.l_fullname - f.l_firstname - f.l_lastname - 1)) 
            WHEN 
                "" THEN NULL 
                ELSE TRIM(SUBSTR(f.middle_lastname, 1, f.l_fullname - f.l_firstname - f.l_lastname - 1)) 
            END as middlename 
            -- ## NOTE this gives "" for middlename when it is missing 
        , f.fieldofstudy
        , g.keywords
        , g.coauthors
        , g.institution
    FROM (
        SELECT a.AuthorId
            , a.YearFirstPub AS year
            , a.FirstName AS firstname
            , REPLACE(b.NormalizedName, RTRIM(b.NormalizedName, REPLACE(b.NormalizedName, " ", "")), "") AS lastname 
                    -- https://stackoverflow.com/questions/21388820/how-to-get-the-last-index-of-a-substring-in-sqlite
            , TRIM(SUBSTR(b.NormalizedName, length(a.FirstName) + 1)) AS middle_lastname 
                    -- this gives all except the first name 
            , length(b.NormalizedName) as l_fullname 
            , length(a.FirstName) as l_firstname
            , length(REPLACE(b.NormalizedName, RTRIM(b.NormalizedName, REPLACE(b.NormalizedName, " ", "")), "")) as l_lastname
            , e.NormalizedName AS fieldofstudy
        FROM author_sample AS a
        INNER JOIN (
            SELECT AuthorId, NormalizedName
            FROM Authors
        ) AS b USING(AuthorId)
        INNER JOIN (
            SELECT AuthorId, NormalizedName
            FROM author_fields c
            INNER JOIN (
                SELECT FieldOfStudyId, NormalizedName
                FROM FieldsOfStudy
            ) AS d USING(FieldOfStudyId)
            -- ## Condition on fieldofstudy being in the level 0 id_field
            INNER JOIN (
                SELECT ParentFieldOfStudyId, ChildFieldOfStudyId
                FROM crosswalk_fields
                WHERE ParentLevel = 0
                    AND ParentFieldOfStudyId IN ({insert_field_questionmarks})
            ) AS e ON (e.ChildFieldOfStudyId = c.FieldOfStudyId)
            WHERE FieldClass = 'first'
        ) AS e USING(AuthorId)
    ) f
    LEFT JOIN (
        SELECT AuthorId
                , institutions as institution
                , coauthors
                , keywords
        FROM author_info_linking
    ) AS g USING(AuthorId)
    {where_stmt_mag} 
    """
elif args.linking_type == "advisors" or args.linking_type == "grants":
    # note: this still sources field of study, but it is level 0 and thus the same for everyone 
    query_mag = f"""
    SELECT f.AuthorId
        , f.year
        , f.firstname
        , f.lastname
        , CASE TRIM(SUBSTR(f.middle_lastname, 1, f.l_fullname - f.l_firstname - f.l_lastname - 1)) 
            WHEN 
                "" THEN NULL 
                ELSE TRIM(SUBSTR(f.middle_lastname, 1, f.l_fullname - f.l_firstname - f.l_lastname - 1)) 
            END as middlename 
            -- ## NOTE this gives "" for middlename when it is missing 
        , f.fieldofstudy
        , g.keywords
        , g.coauthors
        , g.institution
    FROM (
        SELECT a.AuthorId
            , a.YearFirstPub AS year
            , a.FirstName AS firstname
            , REPLACE(b.NormalizedName, RTRIM(b.NormalizedName, REPLACE(b.NormalizedName, " ", "")), "") AS lastname 
                    -- https://stackoverflow.com/questions/21388820/how-to-get-the-last-index-of-a-substring-in-sqlite
            , TRIM(SUBSTR(b.NormalizedName, length(a.FirstName) + 1)) AS middle_lastname 
                    -- this gives all except the first name 
            , length(b.NormalizedName) as l_fullname 
            , length(a.FirstName) as l_firstname
            , length(REPLACE(b.NormalizedName, RTRIM(b.NormalizedName, REPLACE(b.NormalizedName, " ", "")), "")) as l_lastname
            , e.NormalizedName AS fieldofstudy
        FROM author_sample AS a
        INNER JOIN (
            SELECT AuthorId, NormalizedName
            FROM Authors
        ) AS b USING(AuthorId)
        INNER JOIN (
            -- ## mark: different from linking graduates. filter on field0
            SELECT AuthorId, NormalizedName
            FROM author_fields c
            INNER JOIN (
                SELECT FieldOfStudyId, NormalizedName
                FROM FieldsOfStudy
            ) AS d USING(FieldOfStudyId)
            WHERE FieldClass = 'main'
                AND FieldOfStudyId IN ({insert_field_questionmarks})
        ) AS e USING(AuthorId)
    ) f
    LEFT JOIN (
        SELECT AuthorId
                , institutions_career as institution
                , coauthors
                , keywords
        FROM author_info_linking
    ) AS g USING(AuthorId)
    INNER JOIN (
        SELECT AuthorId
        FROM current_links
    ) AS f USING(AuthorId)
    {where_stmt_mag} 
    """

    if args.linking_type == "advisors":
        query_proquest = f"""
        SELECT relationship_id
                , year
                , firstname 
                , lastname
                , CASE TRIM(SUBSTR(middle_lastname, 1, l_fullname-l_firstname-l_lastname - 1)) 
                    WHEN 
                        "" THEN NULL 
                        ELSE TRIM(SUBSTR(middle_lastname, 1, l_fullname-l_firstname-l_lastname - 1)) 
                    END AS middlename
                , fieldofstudy
                , keywords
                , institution
        FROM (
            SELECT goid
                , relationship_id
                , degree_year AS year 
                , a.fullname 
                , SUBSTR(TRIM(a.fullname),1,instr(trim(a.fullname)||' ',' ')-1) AS firstname
                , REPLACE(a.fullname, RTRIM(a.fullname, REPLACE(a.fullname, " ", "")), "") AS lastname 
                , TRIM(SUBSTR(a.fullname, length(SUBSTR(TRIM(a.fullname),1,instr(trim(a.fullname)||' ',' ')-1)) + 1)) AS middle_lastname 
                , length(a.fullname) AS l_fullname 
                , length(SUBSTR(TRIM(a.fullname),1,instr(trim(a.fullname)||' ',' ')-1) ) AS l_firstname
                , length(REPLACE(a.fullname, RTRIM(a.fullname, REPLACE(a.fullname, " ", "")), "")) AS l_lastname
                , fieldname AS fieldofstudy
                , university_id
            FROM pq_authors 
            INNER JOIN (
                SELECT goid, fieldname 
                FROM pq_fields_mag
                WHERE mag_field0 IN ({insert_field_questionmarks})
            ) USING (goid)
            INNER JOIN ( --# NOTE: this only keeps the theses where at least one advisor is present
                SELECT *, firstname || ' ' || lastname AS fullname
                FROM pq_advisors
            ) AS a USING(goid)
        )
        -- ## NOTE: use left join here as not all graduates have advisor (particularly pre-1980) and possibly also keywords
        LEFT JOIN pq_keywords USING(goid) 
        INNER JOIN (
            SELECT university_id, normalizedname as institution
            FROM pq_unis --## mark: different from linking graduates. keep advisors outside the U.S
        ) USING(university_id)
        {where_stmt}
        """
    elif args.linking_type == "grants":
        query_nsf = """
        SELECT a.GrantID, a.Position, SUBSTR(a.Award_AwardEffectiveDate, 7, 4) as Year
            , b.*, c.*
        FROM NSF_MAIN as a 
        INNER JOIN (
            SELECT GrantID, Position, Name 
            FROM NSF_Institution
        ) b 
        USING (GrantID, Position)
        INNER JOIN (
            SELECT GrantID, Position, Firstname, LastName, PIFullName
            FROM NSF_Investigator
            WHERE RoleCode = 'Principal Investigator'
        ) c
        USING (GrantID, Position)
        WHERE AWARD_TranType = "Grant" AND AWARD_Agency = 'NSF' 
        """

  