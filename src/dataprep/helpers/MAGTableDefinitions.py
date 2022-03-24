
MAGtables_setup = {
    "Authors": {
        "rawfile": "mag/Authors.txt",
        "sql_create_table": """
            CREATE TABLE Authors(
                AuthorId INTEGER,
                ImportanceRank INTEGER, 
                NormalizedName TEXT,
                DisplayName TEXT,
                LastKnownAffiliationId INTEGER,
                PaperCount INTEGER,
                PaperFamilyCount INTEGER,
                CitationCount INTEGER,
                CreatedDate TEXT
                )
            """,
        "sql_create_index": """
            CREATE UNIQUE INDEX idx_a_AuthorId 
                ON Authors (AuthorId ASC)
            """
    },
    "Papers": {
        "rawfile": "mag/Papers.txt",
        "sql_create_table": """
            CREATE TABLE Papers(
                PaperId INTEGER,
                ImportanceRank INTEGER, 
                Doi TEXT,
                DocType TEXT,
                PaperTitle TEXT,
                OriginalTitle TEXT,
                BookTitle TEXT,
                Year INTEGER,
                Date TEXT,
                OnlineDate TEXT,
                Publisher TEXT,
                JournalId INTEGER,
                ConferenceSeriesId INTEGER,
                ConferenceInstanceId INTEGER,
                Volume TEXT,
                Issue TEXT,
                FirstPage TEXT,
                LastPage TEXT,
                ReferenceCount INTEGER,
                CitationCount INTEGER,
                EstimatedCitation INTEGER,
                OriginalVenue INTEGER,
                FamilyId INTEGER,
                FamilyRank INTEGER,
                DocSubTypes TEXT,
                CreatedDate TEXT
                )
            """,
        "sql_create_index": """
            CREATE UNIQUE INDEX idx_p_PaperId
                ON Papers (PaperId ASC)
            """
    },
    "FieldsOfStudy": {
        "rawfile": "advanced/FieldsOfStudy.txt",
        "sql_create_table": """
            CREATE TABLE FieldsOfStudy(
                FieldOfStudyId INTEGER,
                ImportanceRank INTEGER,
                NormalizedName TEXT,
                DisplayName TEXT,
                MainType TEXT, 
                Level INTEGER,
                PaperCount INTEGER,
                PaperFamilyCount INTEGER,
                CitationCount INTEGER,
                CreatedDate TEXT
                )
            """,
        "sql_create_index": """
            CREATE UNIQUE INDEX idx_fos_FieldOfStudyId
                ON FieldsOfStudy (FieldOfStudyId ASC)
            """
    },
    "FieldOfStudyChildren": {
        "rawfile": "advanced/FieldOfStudyChildren.txt",
        "sql_create_table": """
            CREATE TABLE FieldOfStudyChildren(
                FieldOfStudyId INTEGER,
                ChildFieldOfStudyId INTEGER
                )
            """,
        "sql_create_index": """
            CREATE INDEX idx_fosc_FieldOfStudyId
                ON FieldOfStudyChildren (FieldOfStudyId ASC)
            """
    },
    "PaperFieldsOfStudy": {
        "rawfile": "advanced/PaperFieldsOfStudy.txt",
        "sql_create_table": """
            CREATE TABLE PaperFieldsOfStudy(
                PaperId INTEGER,
                FieldOfStudyId INTEGER,
                Score NUMERIC
                )
            """,
        "sql_create_index": """
            CREATE UNIQUE INDEX idx_pfos_PaperIdFoS
                ON PaperFieldsOfStudy (PaperId ASC, FieldOfStudyId ASC)
            """
    },
    "PaperAuthorAffiliations": {
        "rawfile": "mag/PaperAuthorAffiliations.txt",
        "sql_create_table": """
            CREATE TABLE PaperAuthorAffiliations(
                PaperId INTEGER,
                AuthorId INTEGER,
                AffiliationId INTEGER,
                AuthorSequenceNumber INTEGER,
                OriginalAuthor TEXT,
                OriginalAffiliation TEXT
                )
            """,
        "sql_create_index": """
            CREATE INDEX idx_paa_PaperIdAuthorIdAffiliationId
                ON PaperAuthorAffiliations (PaperId ASC, AuthorId ASC, AffiliationId ASC)
            """
    },
    "PaperReferences": {
        "rawfile": "mag/PaperReferences.txt",
        "sql_create_table": """
            CREATE TABLE PaperReferences(
                PaperId INTEGER,
                PaperReferenceId INTEGER
                )
            """,
        "sql_create_index": """
            CREATE UNIQUE INDEX idx_pr_PaperIdReferenceId
                ON PaperReferences (PaperId ASC, PaperReferenceId ASC)
            """
    },
    "Affiliations": {
        "rawfile": "mag/Affiliations.txt",
        "sql_create_table": """
            CREATE TABLE Affiliations(
                AffiliationId INTEGER,
                ImportanceRank INTEGER,
                NormalizedName TEXT,
                DisplayName TEXT,
                GridId TEXT,
                OfficialPage TEXT,
                WikiPage TEXT,
                PaperCount INTEGER,
                PaperFamilyCount INTEGER,
                CitationCount INTEGER,
                Iso3166Code TEXT,
                Latitude NUMERIC,
                Longitude NUMERIC,
                CreatedDate TEXT
                )
            """,
        "sql_create_index": """
            CREATE INDEX idx_a_AffiliationId
                ON Affiliations (AffiliationId ASC)
            """
    }
}


