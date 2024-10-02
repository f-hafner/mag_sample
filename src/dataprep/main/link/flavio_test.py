
import ibis 
from ibis import _

db_file = "/mnt/ssd/AcademicGraph/AcademicGraph.sqlite" 
ibis.options.interactive = True
con = ibis.connect(db_file)

max_level = 2
field_to_query = 95457728

links_to_cng = con.table("links_to_cng")
affiliations = con.table("affiliations")
fields_of_study = con.table("FieldsOfStudy")

current_links = con.table("current_links")
pq_authors = con.table("pq_authors")
pq_fields_mag = con.table("pq_fields_mag")
pq_magfos = con.table("pq_magfos")

author_affiliation = con.table("AuthorAffiliation")
author_fields = con.table("author_fields")

degree_year_to_query = 2001

paper_author_unique = con.table("PaperAuthorUnique")
papers = con.table("Papers")
author_fields_detailed = con.table("author_fields_detailed")

affiliation_fields = con.table("affiliation_fields")
affiliation_outcomes = con.table("affiliation_outcomes")

query_affiliations = (
    affiliations
    .join(
        links_to_cng,
        affiliations.AffiliationId == links_to_cng.from_id
    )
    .filter(
        links_to_cng.from_dataset == "mag"
    )
    .select(
        _.AffiliationId
    )
)

query_fields_up_to_max_level = (
    fields_of_study
    .filter(
        _.Level <= max_level
    )
    .select(
        _.FieldOfStudyId
    )
)

query_graduates = (
    current_links
    .join(
        pq_authors,
        "goid",
        how="inner"
    )
    .filter(
        pq_authors.degree_year == degree_year_to_query
    )
    .join(
        pq_fields_mag
        .mutate(
            row_num=ibis
                .row_number()
                .over(group_by=_.goid, order_by=_.position.asc()
            )
        )
        .filter(
            [_.row_num == 0, _.mag_field0 == field_to_query]
        )
        .select(
            _.goid,
            _.position,
            Field0 = _.mag_field0
        ),
        "goid"
    )
    .select(
        _.goid,
        _.AuthorId,
        _.degree_year,
        _.Field0
    )
)
        
query_topics_dissertation = (
    pq_magfos
    .join(
        query_graduates,
        "goid",
        how="inner"
    )
    .join(
        query_fields_up_to_max_level,
        "FieldOfStudyId",
        how="inner"
    )
)

#topics_dissertation = query_topics_dissertation.execute() # this allows us to compute the first topic vector
# need to extend it to "all" possible topics, and pivot_wide
# in 2001, we have 686 concepts? 

query_collaborators = (
    author_affiliation
    .join(
        query_affiliations,
        "AffiliationId",
        how="inner"
    )
    .join(
        author_fields
        .filter(
            [_.FieldClass == "main", _.FieldOfStudyId == field_to_query]
        )
        .select(
            _.AuthorId
        ),
        "AuthorId",
        how="inner"
    ).
    filter(
        [_.Year <= degree_year_to_query + 5, # TODO: careful with years here!
         _.Year >= degree_year_to_query - 5] 
    )
)

query_author_papers = (
    paper_author_unique
    .join(
        papers
        .filter(
            _.DocType == "Journal"
        )
        .select(
            _.PaperId,
            _.Year
        ),
        "PaperId",
        how="inner"
    )
    .join(
        query_collaborators
        .select(_.AuthorId),
        "AuthorId",
        how="inner"
    )
    .filter(
        [_.Year <= degree_year_to_query + 5,
        _.Year >= degree_year_to_query - 5]
    )
    .select(
        _.PaperId,
        _.AuthorId,
        _.Year
    )
)




query_collaborators_topics = (
    author_fields_detailed
    .join(
        author_fields
        .filter(
            _.FieldClass == "main"
        )
        .select(
            _.AuthorId,
            Field0 = _.FieldOfStudyId
        ),
        "AuthorId",
        how="inner"
    ).
    join(
        query_collaborators
        .select(_.AuthorId),
        "AuthorId",
        how="inner"
    )
    .join(
        query_fields_up_to_max_level,
        "FieldOfStudyId",
        how="inner"
    )
    .mutate(
        Score = _.Score / _.PaperCount
    )
    .filter(
        [_.Year <= degree_year_to_query + 5,
         _.Year >= degree_year_to_query - 5]
    )
    .select(
        _.AuthorId,
        _.FieldOfStudyId,
        _.Year,
        _.Field0,
        _.PaperCount,
        _.Score
    )
)


query_affiliation_topics = (
        affiliation_fields
        .join(
            affiliation_outcomes
            .select(
                _.AffiliationId,
                _.Field0,
                _.Year,
                _.PaperCount
            ),
            ["AffiliationId", "Year", "Field0",
             affiliation_fields.Field0 == field_to_query,
             affiliation_outcomes.Year <= degree_year_to_query + 5,
             affiliation_outcomes.Year >= degree_year_to_query - 5],
            how="inner"
        )
        .join(
            query_affiliations,
            "AffiliationId",
            how="inner"
        )
        .select(
            _.AffiliationId,
            _.Field0,
            _.Year,
            _.FieldOfStudyId,
            Score = _.Score / _.PaperCount
        )
)

## need a reference: each FieldOfStudyId, each AffiliationId, each period (pre/post)
# focus on pre for now?






