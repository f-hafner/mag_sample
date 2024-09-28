
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
        affiliations.AffiliationId
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
        pq_authors.degree_year == 2001
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

topics_dissertation = query_topics_dissertation.execute() # this allows us to compute the first topic vector
# need to extend it to "all" possible topics, and pivot_wide
# in 2001, we have 686 concepts? 






