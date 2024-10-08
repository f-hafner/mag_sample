import pandas as pd 
import numpy as np 

import main.link.similarity_helpers as sim_helpers
import logging 


class QueryBuilder():
    """Construct sql queries for similarity computations

    Args:
        max_level: maximum level for FieldsOfStudy for computing the concept
        vectors
    """
    def __init__(
        self, 
        degree_year_to_query,
        window_size, 
        field_to_query,
        qmarks_doctypes,
        keep_doctypes,
        max_level):

        self.degree_year_to_query = degree_year_to_query
        self.window_size = window_size
        self.field_to_query = field_to_query
        self.qmarks_doctypes = qmarks_doctypes 
        self.keep_doctypes = keep_doctypes
        self.year_restriction = f"""
            WHERE Year <= {degree_year_to_query} + {window_size}
            AND Year >= {degree_year_to_query} - {window_size}
        """
        self.max_level = max_level
   
    def query_affiliations(self):
        q = """
            SELECT AffiliationId
            FROM affiliations a
            INNER JOIN (
                SELECT from_id, unitid 
                FROM links_to_cng
                WHERE from_dataset = 'mag'
            ) b
            ON a.AffiliationId = b.from_id
        """
        return q 
    
    def query_fields_up_to_max_level(self):
        q = f"""
            SELECT FieldOfStudyId
            FROM FieldsOfStudy
            WHERE Level <= {self.max_level}
            """
        return q


    def query_affiliation_size(self, affiliation_ids_to_query):
        affiliation_ids_to_query = ", ".join(str(i) for i in affiliation_ids_to_query)
        q = f"""
            SELECT AffiliationId, AuthorCount
            FROM affiliation_outcomes
            WHERE AffiliationId IN ({affiliation_ids_to_query})
                AND Field0 = {self.field_to_query}
                AND Year = {self.degree_year_to_query}
        """
        return q 
            
    def query_graduates(self):
        q = f"""
            SELECT goid, AuthorId, degree_year, Field0
            FROM current_links
            INNER JOIN (
                SELECT goid, degree_year
                FROM pq_authors
                WHERE degree_year = {self.degree_year_to_query}
            ) USING(goid)
            INNER JOIN (
                SELECT goid, Field0
                FROM (
                    SELECT goid
                        , position
                        , mag_field0 AS Field0
                        , MIN(position) OVER(PARTITION BY goid) as min_position
                    FROM pq_fields_mag
                )
                WHERE position = min_position
                    AND field0 = {self.field_to_query}
            ) USING(goid)
        """
        return q 
   
    def query_topics_dissertation(self):
        q = f"""
            SELECT AuthorId, FieldOfStudyId, score AS Score
            FROM pq_magfos 
            INNER JOIN (
                {self.query_graduates()}
            ) USING(goid)
            INNER JOIN (
                {self.query_fields_up_to_max_level()}
            ) USING(FieldOfStudyId) 
        """
        return q 

    def query_topics_postphd(self):
        q = f"""
            SELECT PaperId
                , AuthorId
                , goid
                , FieldOfStudyId
                , Score
            FROM PaperAuthorUnique a 
            INNER JOIN (
                SELECT PaperId, Year
                FROM Papers
                WHERE DocType IN ({self.qmarks_doctypes})
            ) b 
            USING(PaperId)
            INNER JOIN (
                {self.query_graduates()}
            ) c
            USING(AuthorId)
            INNER JOIN (
                SELECT PaperId, FieldOfStudyId, Score
                FROM PaperFieldsOfStudy
                INNER JOIN (
                    {self.query_fields_up_to_max_level()}
                ) USING(FieldOfStudyId)
            ) d
            USING(PaperId)
            WHERE Year <= degree_year + {self.window_size}
                AND Year > degree_year
                AND Score > 0
            """
        return q

    def query_collaborators(self, affiliation_ids_to_query=None):
        q = f"""
            SELECT *
            FROM AuthorAffiliation 
            INNER JOIN (
                {self.query_affiliations()}
            ) USING(AffiliationId)
            INNER JOIN (
                SELECT AuthorId
                FROM author_fields
                WHERE FieldClass = 'main'
                    AND FieldOfStudyId = {self.field_to_query}
            ) c
            USING(AuthorId)
            {self.year_restriction}
        """
        if affiliation_ids_to_query is not None:
            assert isinstance(affiliation_ids_to_query, list)
            affiliation_ids_to_query = ", ".join(str(i) for i in affiliation_ids_to_query)
            q = f"{q} AND AffiliationId IN ({affiliation_ids_to_query})"
        
        return q 
    
    def query_author_papers(self, affiliation_ids_to_query=None):
        "Extract papers of relevant authors."
        keep_authors = f"""
            SELECT AuthorId 
            FROM (
                {self.query_collaborators(affiliation_ids_to_query=affiliation_ids_to_query)}
            )
        """
        q = f"""
            SELECT PaperId, AuthorId, Year 
            FROM PaperAuthorUnique 
            INNER JOIN (
                SELECT PaperId, Year 
                FROM Papers 
                WHERE DocType IN ({self.qmarks_doctypes})
            ) USING(PaperId)
            {self.year_restriction} 
                AND AuthorId IN (
                    {keep_authors}
                )
        """
        return q 
    
    def query_collaborators_topics(self, author_ids_to_query):
        author_ids_to_query = ", ".join(str(i) for i in author_ids_to_query)
        q = f"""
            SELECT AuthorId
            , FieldOfStudyId
            , Year
            , Score / PaperCount AS Score
            , Field0
            , PaperCount
        FROM author_fields_detailed a
        INNER JOIN (
            SELECT AuthorId, FieldOfStudyId AS Field0
            FROM author_fields
            WHERE FieldClass = 'main'
        ) c
        USING(AuthorId)
        INNER JOIN (
            {self.query_fields_up_to_max_level()}
        ) USING(FieldOfStudyId)
        {self.year_restriction}
        AND AuthorId IN ({author_ids_to_query})
        """
        return q 
    
    def query_affiliation_topics(self):

        q = f"""
            SELECT a.AffiliationId
                , a.Field0
                , a.Year
                , a.FieldOfStudyId
                , a.Score / c.PaperCount AS Score
            FROM affiliation_fields a
            INNER JOIN (
                {self.query_affiliations()}
            ) b USING(AffiliationId)
            INNER JOIN (
                SELECT AffiliationId
                    , Field0
                    , Year
                    , PaperCount
                FROM affiliation_outcomes
            ) c
            ON a.AffiliationId = c.AffiliationId
                AND a.Year = c.Year
                AND a.Field0 = c.Field0
                {self.year_restriction}
                AND a.Field0 = {self.field_to_query}
        """
        return q 


## Support functions 

def compute_similarity(df_A, df_B, unit_A, unit_B, groupvars, fill_A_units = False, debug=False):
    """Compute similarity between records in df_A and in df_B.
    unit_A and unit_B refer to column names defining the units of observation.
    groupvars define other grouping variables common in df_A and df_B.
    """
    df_A = df_A.rename(columns={"Score": "A"})
    df_B = df_B.rename(columns={"Score": "B"})

    if debug:
        breakpoint()

    d_AA = (df_A 
        .assign(AA=lambda x: x.A**2)
        .groupby(sim_helpers.unique(unit_A + groupvars))
        .agg({"AA": np.sum})
    )
    d_BB = (df_B 
        .assign(BB=lambda x: x.B**2)
        .groupby(sim_helpers.unique(unit_B + groupvars))
        .agg({"BB": np.sum})
        )
    d_AB = (df_B
        .set_index(sim_helpers.unique(groupvars + ["FieldOfStudyId"]))
        .join(df_A
                .set_index(sim_helpers.unique(groupvars + ["FieldOfStudyId"])), 
                how="inner")
        .reset_index()
    )
    d_AB = (d_AB
        .assign(AB=lambda x: x.A * x.B)
        .groupby(sim_helpers.unique(unit_A + unit_B + groupvars))
        .agg({"AB": np.sum})
        .reset_index()
        .set_index(list(d_AA.index.names))
        .join(d_AA)
        .reset_index()
        .set_index(list(d_BB.index.names))
        .join(d_BB)
        .reset_index()
    )
    d_AB = sim_helpers.cosine_similarity_on_df(d_AB)

    if fill_A_units:
        # fill all units in df_A with similarity of 0
        required_ids = pd.DataFrame(d_AA.reset_index()[unit_A].squeeze().unique())
        required_ids.columns = unit_A
        d_AB = (required_ids
            .set_index(unit_A)
            .join(d_AB.set_index(unit_A))
            .reset_index()
        )
        d_AB = sim_helpers.fill_nas(d_AB, ["sim"])

    outvars = unit_A + unit_B + groupvars + ["sim"]
    return d_AB.loc[:, sim_helpers.unique(outvars)]


def complete_to_reference(
    df_in, 
    df_ref, 
    idx_cols,
    add_cols_to_complete,
    ignore_column="Field0",
    fill_value=0
    ):
    """Complete a dataframe `df_in` relative to a reference dataframe `df_ref`.
    
    Parameters:
    ----------
    `df_in`, `df_ref: dataframes
    `idx_cols`: index columns to join the two dfs by. 
    `add_cols_to_complete`: additional columns over which to complete the dataframe
    `ignore_column`: ignore this column for the "complete" operation
    `fill_value`: fill value passed as `fill_value` to `pd.MultiIndex.from_product()`
    """
    cols_to_complete = idx_cols + add_cols_to_complete
    flds = df_ref[ignore_column].unique()
    ls_out = []
    for fld in flds:
        # join 
        d_full = (
            df_ref
                .loc[df_ref[ignore_column]==fld, idx_cols]
                .set_index(idx_cols)
                .join(df_in
                        .drop(columns=[ignore_column])
                        .set_index(idx_cols))
                .reset_index()
        )
        # "complete"
        d_full = d_full.set_index(cols_to_complete)
        mux = pd.MultiIndex.from_product(
            [lvl for lvl in d_full.index.levels],
            names=cols_to_complete
        )
        d_full = d_full.reindex(mux, fill_value=fill_value).reset_index()
        d_full[ignore_column] = fld
        ls_out.append(d_full)


    return pd.concat(ls_out)


## Main functions here 

def get_student_data(con, queries):
    """Prepare main data at student level
    
    Parameters:
    ----------
    con: sqlite connection
    queries: QueryBuilder instance
    
    """
    with con as c:
        topics_dissertation = pd.read_sql(
            con=c, 
            sql=queries.query_topics_dissertation()
        )
        topics_postphd = pd.read_sql(
            con=c, 
            sql=queries.query_topics_postphd(),
            params=queries.keep_doctypes
        )
        # general tables for later reference
        d_graduates = pd.read_sql(
            con=c, 
            sql=queries.query_graduates()
        )

    n_papers_postphd = (topics_postphd
                        .groupby(["AuthorId"])
                        .agg({"PaperId": pd.Series.nunique})
                        .rename(columns={"PaperId": "PaperCount"})
                        )

    topics_postphd = (topics_postphd
                        .groupby(["AuthorId", "FieldOfStudyId"])
                        .agg({"Score": np.sum})
                        .reset_index(["FieldOfStudyId"])
                        .join(n_papers_postphd)
                        .reset_index()
                    )
    topics_postphd["Score"] = topics_postphd["Score"] / topics_postphd["PaperCount"]
    topics_postphd = topics_postphd.drop(columns="PaperCount")

    # create pre/post df for similarity calculations below 
    topics_postphd["period"] = "post_phd"
    topics_dissertation["period"] = "pre_phd"
    student_topics = pd.concat(
        [topics_postphd, topics_dissertation]
    )
    student_topics = (student_topics
        .set_index(["AuthorId"])
        .join(d_graduates
                .loc[:, ["AuthorId", "Field0"]]
                .set_index(["AuthorId"])
                )
        .reset_index()
    )
    return (student_topics, d_graduates)

def make_student_affiliation_table(d_affiliations, d_graduates):
    """Make reference table with all student-affiliation combinations 
    which needs to be filled with data at the end

    Parameters:
    ----------
    d_affiliations: dataframe with hiring AffiliationIds 
    d_graduates: dataframe with goid, AuthorId, degree year and Field0

    """
    d_affiliations["key"] = 0 
    d_field0 = pd.DataFrame(d_graduates["Field0"].unique())
    d_field0.columns = ["Field0"]
    d_field0["key"] = 0
    d_affiliations_fields = (d_affiliations
        .set_index(["key"])
        .join(d_field0.set_index(["key"]), 
                on="key", 
                how="outer")
        .reset_index()
        )
    d_graduates["key"] = 0 
    d_out = (d_affiliations_fields
        .set_index(["Field0", "key"])
        .join(d_graduates
                .loc[:, ["AuthorId", "Field0", "key"]]
                .set_index(["Field0", "key"]),
                on=["Field0", "key"],
                how="outer"
                )
        .reset_index()
        .drop(columns=["key"])
        )

    n_grads_affils = d_out.groupby(["AffiliationId", "AuthorId"]).ngroups
    assert n_grads_affils == d_out.shape[0], "AuthorId-AffiliationId not unique"

    return d_out

def similarity_to_faculty(
        d_affiliations, 
        d_graduates,
        student_topics,
        queries,
        con
    ):
    """Calculate similarity between student topics and overall faculty topics.

    Parameters:
    -----------
    d_affiliations: dataframe with hiring AffiliationIds 
    d_graduates: dataframe with goid, AuthorId, degree year and Field0
    student_topics: dataframe with scores by AuthorId, FieldOfStudyId, period and Field0
    queries: QueryBuilder instance
    con: sqlite connection
    """

    # Get affiliation topics 
    with con as c:
        df_fields = pd.read_sql(con=c, sql=queries.query_affiliation_topics())
    
    df_fields = sim_helpers.split_year_pre_post(df=df_fields, ref_year=queries.degree_year_to_query)

    affiliation_topics = (df_fields
        .groupby(["AffiliationId", "Field0", "FieldOfStudyId", "period"])
        .agg({"Score": np.sum})
        .reset_index()
        )

    # calculate similarity 
    d_sim = compute_similarity(
        df_A=student_topics, 
        df_B=affiliation_topics,
        unit_A=["AuthorId"],
        unit_B=["AffiliationId"], 
        groupvars=["period", "Field0"])

    # "reference" table 
    d_graduates_affiliations = make_student_affiliation_table(
        d_affiliations=d_affiliations,
        d_graduates=d_graduates
    )
    d_sim = complete_to_reference(
        df_in=d_sim, 
        df_ref=d_graduates_affiliations,
        idx_cols=["AuthorId", "AffiliationId"], 
        add_cols_to_complete=["period"]
    )

    # NOTE: should Field0 be dropped? it is based on the student's field of study id.. 
        # but it is good to keep in mind *based on what data* the statistics were calculated
    return d_sim


def similarity_to_closest_collaborator(
        con,
        queries,
        student_topics,
        d_affiliations,
        d_graduates,
        top_n_authors=200,
        max_nrow_input_similarity=10_000_000
    ):
    """Calcuate highest similarity between students among potential coauthors, for all potential
    destination institutions.

    Parameters:
    -----------
    con: sqlite connection
    quries: QueryBuilder instance
    student_topics: dataframe with scores by AuthorId, FieldOfStudyId, period, Field0
    d_affiliations, d_graduates: dataframes with affiliations and graduates
    top_n_authors: For each institution, only consider top_n_authors by number of papers
        in a given time period (defined in queries.year_restriction.)
    max_nrow_input_similarity: Maximum number of rows to be processed by compute_similarity.
        Chunks of affiliation ids are processed sequentially to reduce
        memory of each operation.
    """

    # 1. Get data 
    # logging.debug(f"querying db for affiliations.")
    with con as c:
        collaborators_affiliations = pd.read_sql(
            con=c, 
            sql=queries.query_collaborators())
        collaborators_papers = pd.read_sql(
            con=c,
            sql=queries.query_author_papers(),
            params=queries.keep_doctypes
        )

    collaborators_affiliations = sim_helpers.split_year_pre_post(
        df=collaborators_affiliations, 
        ref_year=queries.degree_year_to_query
    )
    collaborators_papers = sim_helpers.split_year_pre_post(
        df=collaborators_papers, 
        ref_year=queries.degree_year_to_query
    )

    # 2. a. Find first and last affiliation relative to the PhD year of the graduates
    collaborators_affiliations["diff"] = np.abs(
        collaborators_affiliations["Year"] - queries.degree_year_to_query
    )
    collaborators_affiliations["min_diff"] = (collaborators_affiliations
        .groupby(["AuthorId", "period"])["diff"]
        .transform("min")
    )
    collaborators_affiliations = collaborators_affiliations.loc[
        collaborators_affiliations["min_diff"] == collaborators_affiliations["diff"],
        ["AuthorId", "AffiliationId", "period"]
    ]
    collaborators_affiliations = (collaborators_affiliations
        .drop_duplicates()
        )

    # 2.b. Find top n authors by papercount for each affiliation
    collaborators_papercount = (
        collaborators_papers
            .groupby(["AuthorId", "period"])
            .agg({"PaperId": pd.Series.nunique})
            .rename(columns={"PaperId": "PaperCount"})
    )

    logging.debug(f"top_n_authors is {top_n_authors}")
    d_top_collaborators = (
        collaborators_affiliations
            .set_index(list(collaborators_papercount.index.names))
            .join(collaborators_papercount)
            .reset_index()
            .set_index(["AuthorId"])
            .groupby(["period", "AffiliationId"])
            ["PaperCount"]
            .nlargest(top_n_authors)
            .reset_index()
            .rename(columns={"AuthorId": "CoAuthorId"})
            .drop(columns=["PaperCount"])
    )

    collaborators_to_query = list(d_top_collaborators["CoAuthorId"].unique())

    # 3. query the topics of these authors. 
        # NOTE: already conditional on field_to_query b/c of restriction to AuthorIds
    # logging.debug("querying db for topics of collaborators")
    with con as c:
        topics_collaborators = pd.read_sql(
            con=c, 
            sql=queries.query_collaborators_topics(author_ids_to_query=collaborators_to_query)
        )

    # 4. aggregate pre/post, by field 
    topics_collaborators = sim_helpers.split_year_pre_post(
        df=topics_collaborators, 
        ref_year=queries.degree_year_to_query
    )
    topics_collaborators = (topics_collaborators
        .groupby(["AuthorId", "Field0", "period", "FieldOfStudyId"])
        .agg({"Score": np.sum})
        .reset_index()
        .rename(columns={"AuthorId": "CoAuthorId"})
        )

    topics_collaborators_affiliations = (d_top_collaborators
        .set_index(["CoAuthorId", "period"])
        .join(topics_collaborators
            .set_index(["CoAuthorId", "period"]),
            how="left")
        .reset_index()
        )
    
    size_graduates = student_topics.shape[0] 
    topics_collaborators_affiliations = make_itergroups(
        df=topics_collaborators_affiliations,
        groupcol=["AffiliationId"],
        max_size=int(max_nrow_input_similarity / size_graduates),
        new_colname="itergroup"
    )

    compare_groups = (
        f"""Have {topics_collaborators_affiliations['itergroup'].nunique()} itergroups """
        f"""and {topics_collaborators_affiliations['AffiliationId'].nunique()} affiliation ids"""
    )
    logging.debug(compare_groups)

    logging.debug(f"computing similarity between graduates and collaborators")
    d_sim = []
    for n, g in topics_collaborators_affiliations.groupby("itergroup"):
        dtemp = compute_similarity(
                df_A=student_topics,
                df_B=g,
                unit_A=["AuthorId"],
                unit_B=["CoAuthorId", "AffiliationId"],
                groupvars=["Field0", "period"]
                )
        d_sim.append(dtemp)
    
    d_sim = pd.concat(d_sim)
        
    
    # 5. calculate individual similarity, keep most similar 
    # logging.debug("max similarity between graduates and institutions")
    d_sim["max_sim"] = (
        d_sim
            .groupby(["AuthorId", "Field0", "period", "AffiliationId"])["sim"]
            .transform("max")
    )

    d_most_similar_collaborator = (
        d_sim.loc[
            d_sim["sim"] == d_sim["max_sim"], 
            ["AuthorId", "AffiliationId", "CoAuthorId", "period", "Field0", "sim"]
        ]
    ) # can have multiple at same institution if the similarity is the same 

    # 6. Separate most similar collaborator IDs from max distance 
    # logging.debug("separate most similar collaborator and affiliations")
    idx_vars = ["AuthorId", "Field0", "AffiliationId", "period"]
    sim_most_similar_collaborator_by_affiliation = (
        d_most_similar_collaborator
            .loc[:, idx_vars + ["sim"]]
            .drop_duplicates()
    )
    d_graduates_affiliations = make_student_affiliation_table(
        d_affiliations=d_affiliations,
        d_graduates=d_graduates
    )
    sim_most_similar_collaborator_by_affiliation = complete_to_reference(
        df_in=sim_most_similar_collaborator_by_affiliation,
        df_ref=d_graduates_affiliations,
        idx_cols=["AuthorId", "AffiliationId"], 
        add_cols_to_complete=["period"]
    )

    return d_most_similar_collaborator, sim_most_similar_collaborator_by_affiliation


def make_itergroups(df, groupcol, max_size, new_colname):
    """From a df, make a new column to iterate over, where all rows from a group
    are contained in the same itergroup. 
    Each itergroup should roughly have the same number of rows.

    Parameters
    ----------
    df: pd.DataFrame
        The dataframe on which to generate the column.
    groupcol: str
        The column in `df` whose rows should all be in the same new itergroup.
    max_size: int
        Maximum size of the newly created itergroup.
    new_colname: str
        Name of the new column.
    """
    agg = df.groupby(groupcol).size().reset_index(name="counts")

    newgroups = make_groups(df_to_iter(agg), max_size)

    newgroups = pd.DataFrame(newgroups.items())
    newgroups.columns = groupcol + [new_colname]

    out = (
        df.set_index(groupcol)
            .join(newgroups
                    .set_index(groupcol))
            .reset_index()
    )
    return out 


def make_groups(items, size_max):
    "Make groups of equal size of size_max from items with (identifier, size)"
    count = 0 
    outdict = {}
    n_groups = 0
    for item in items:
        count += item[1]
        outdict[item[0]] = n_groups
        if count >= size_max:
            count = 0
            n_groups += 1
    
    return outdict


def df_to_iter(df):
    "yield itertuples without the index column."
    for i in df.itertuples():
        yield list(i)[1:]



