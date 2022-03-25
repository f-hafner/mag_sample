
## Script that makes various operations on the database. 
  # Results are collected in data_dynamics and data_duration

### Make tables 

# ### MAG
author_panel <- tbl(con, "author_panel")
author_output <- tbl(con, "author_output")
author_citations <- tbl(con, "author_citations")
author_sample <- tbl(con, "author_sample")
FieldsOfStudy <- tbl(con, "FieldsOfStudy")


# ### Proquest
graduates_fields <- tbl(con, "pq_fields_mag") %>%
  select(goid, position, fieldname, mag_field0) %>%
  left_join(FieldsOfStudy %>%
              select(FieldOfStudyId, field_name = NormalizedName),
            by = c("mag_field0" = "FieldOfStudyId"))
pq_authors <- tbl(con, "pq_authors") %>%
  select(goid, degree_year)

# ### gender
names_gender <- tbl(con, "FirstNamesGender")

# ### linked data
drop_links <- tbl(con, "current_links") %>%
  select(AuthorId, goid) %>%
  collect() %>%
  group_by(goid) %>%
  mutate(n_links = n()) %>%
  ungroup() %>%
  filter(n_links > 1) %>%
  pull(goid) %>%
  unique()


query_links <- paste0(
  "SELECT AuthorId, goid, link_score
  FROM current_links 
  WHERE goid NOT IN (",
  paste0(drop_links, collapse = ", "),
  ") AND link_score > 0.7"
)

current_links <- tbl(con, sql(query_links)) 
### Correspond fields in MAG and ProQuest
#- firstfield in MAG to field level 0 using crosswalk 
#- pq fields as below 

# ### PhD advisor links 
advisor_links <- tbl(con, "current_links_advisors") %>%
  inner_join(current_links %>% 
               select(AuthorId), 
             by = "AuthorId") %>%
  inner_join(tbl(con, sql("SELECT goid, relationship_id FROM pq_advisors")),
             by = "relationship_id") %>%
  inner_join(tbl(con, sql("SELECT goid, degree_year AS advisee_degree_year, university_id FROM pq_authors")),
             by = "goid") %>%
  inner_join(tbl(con, sql("SELECT university_id, location AS uni_location, normalizedname as uni_advisee_name FROM pq_unis")),
             by = "university_id") %>%
  select(AuthorId, advisee_degree_year, uni_location, uni_advisee_name)
  


firstfield_to_main <- tbl(con, "crosswalk_fields") %>% # rename this!
  filter(ChildLevel == 1) %>%
  select(ParentFieldOfStudyId, ChildFieldOfStudyId) %>%
  left_join(FieldsOfStudy %>%
              select(FieldOfStudyId, main_field = NormalizedName),
            by = c("ParentFieldOfStudyId" = "FieldOfStudyId")) %>%
  select(ChildFieldOfStudyId, main_field)

# table with linked author id, id of first field (level 1) and name of main_field (level 0)
author_fields <- tbl(con, "author_fields") %>%
  filter(FieldClass %in% c("first")) %>%
  select(AuthorId, first_field_id = FieldOfStudyId) %>%
  inner_join(current_links %>%
               select(AuthorId),
             by = "AuthorId") %>%
  left_join(firstfield_to_main,
            by = c("first_field_id" = "ChildFieldOfStudyId")) %>%
  # define LPS vs GEEMP
  mutate(field_group = case_when(
    main_field %in% lps_fields ~ "LPS",
    main_field %in% geemp_fields ~ "GEEMP"
  ))


graduates_fields <- graduates_fields %>%
  inner_join(current_links,
             by = "goid")


### Joins on db
author_info <- author_sample %>%
  inner_join(names_gender, by = "FirstName") %>%
  mutate(gender = case_when(
    ProbabilityFemale >= threshold_prob_female ~ "Female",
    ProbabilityFemale <= 1 - threshold_prob_female ~ "Male")
  ) %>%
  mutate(censored = ifelse(YearLastPub >= end_year_duration, 1, 0)) %>%
  select(AuthorId, YearFirstPub, gender, censored, YearLastPub) 

author_career <- author_panel %>%
  left_join(author_info,
            by = c("AuthorId")) %>%
  left_join(author_output,
            by = c("AuthorId", "Year")) %>%
  left_join(author_citations,
            by = c("AuthorId", "Year")) %>%
  filter(!is.na(gender)) %>%
  inner_join(current_links, by = "AuthorId") %>%
  left_join(pq_authors, by = "goid") %>%
  mutate(career_length = YearLastPub - degree_year) %>%
  select(-YearLastPub)



fields_names <- FieldsOfStudy %>%
  filter(Level <= 1) %>%
  select(FieldOfStudyId, NormalizedName) 

crosswalk_fields <- tbl(con, "crosswalk_fields") %>%
  filter(ChildLevel == 1) 
