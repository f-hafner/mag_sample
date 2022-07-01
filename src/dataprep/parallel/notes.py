

# example 1: unique author-coauthor by year 
###############################
f"""SELECT a.AuthorId, d.AuthorId AS CoAuthorId, d.Year 
    FROM PaperAuthorUnique AS a
    INNER JOIN (
        SELECT b.*, c.Year 
        FROM PaperAuthorUnique AS b
        INNER JOIN (
            SELECT PaperId, Year 
            FROM Papers
            WHERE DocType IN ({insert_questionmark_doctypes})
        ) AS c
        USING (PaperId)
    ) AS d
    ON (a.PaperId = d.PaperId and a.AuthorId != d.AuthorId)
    -- drop authors not in author_sample
    INNER JOIN (
        SELECT AuthorId
        FROM author_sample
        WHERE AuthorId IN ({qmark_current_ids})
    ) AS e ON (a.AuthorId = e.AuthorId)
    INNER JOIN (
        SELECT AuthorId
        FROM author_sample
    ) AS f on (CoAuthorId = f.AuthorId)
"""
# then apply pd.drop_duplicates


# example 2: paperauthorunique
###############################
f"""    
SELECT a.PaperId, a.AuthorId 
FROM PaperAuthorAffiliations a
INNER JOIN (
    SELECT PaperId 
    FROM PaperMainFieldsOfStudy 
) USING (PaperId)
WHERE a.PaperId IN ({qmark_current_ids})
"""
    # then apply pd.drop_duplicates
    # iterating over paperids


# example 3: author_sample
###############################

# query 1: names
firstnames = f"""select AuthorId, SUBSTR(TRIM(NormalizedName),1,instr(trim(NormalizedName)||' ',' ')-1) AS FirstName
    FROM Authors 
    WHERE AuthorId IN ({qmark_current_ids})
"""

# query 2: authors and papers 
table2 = f"""
SELECT a.AuthorId, b.Year
FROM PaperAuthorUnique 
INNER JOIN (
    SELECT Paperid, DocType, Year
    FROM Papers
)
WHERE b.DocType IN ({insert_questionmark_doctypes})
    AND AuthorId IN ({qmark_current_ids})
"""

# then apply pandas on table2: max(year), min(year), number of papers
    # by AuthorId
    # then keep authors if PaperCount >= 2 and PaperCount / (YearLastPub - YearFirstPub) <= 20 
# join table1
# iterating over authorids

# question:
    # best way to store this information?
    # use classes/objects?




