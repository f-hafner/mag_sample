

# data_dynamics.R
# Load data publication dynamics 



### Collect

author_career <- collect(author_career) %>%
  filter(degree_year %in% 1990:2000)
author_fields <- collect(author_fields) 

# graduates_fields <- collect(graduates_fields)
info_proquest <- tbl(con, "pq_authors") %>%
  inner_join(current_links,
             by = "goid") %>%
  select(AuthorId, degree_year, university_id) %>%
  collect()

fields_names <- collect(fields_names)
crosswalk_field <- collect(crosswalk_fields)


### Define additional variables
author_career <- author_career %>%
  filter(degree_year - YearFirstPub >= -5
         & degree_year - YearFirstPub <= 5) %>%
  mutate(career_start = degree_year,
         YearsExperience = Year - career_start)

# condition on maximum length of experience so that also the late starters (graduated in 2000) have the potential to be present in the full data 
# author_career <- author_career %>%
#   filter(YearsExperience <= 14)



### Aggregate at author-experience level 
author_careerbins <- author_career %>%
  mutate(YearsExperience = size_careerbin * floor(YearsExperience / size_careerbin),
         cohort = 10 * floor(YearFirstPub / 10)) %>%
  group_by(AuthorId, gender, cohort, YearsExperience) %>%
  summarise(TotalForwardCitations = sum(TotalForwardCitations),
            PaperCount = sum(PaperCount),
            .groups = "drop") %>%
  mutate(CitationsPerPaper =
           ifelse(PaperCount > 0,
                  TotalForwardCitations / PaperCount,
                  NA)) %>%
  mutate(PaperCount_intensive = PaperCount,
         TotalForwardCitations_intensive = TotalForwardCitations) %>%
  mutate(
    across(.cols = c("PaperCount", "TotalForwardCitations"),
           .fns = ~ifelse(is.na(.x), 0, .x))
  )


# define the end stage for each career
# author_careerend <- author_career %>%
#   group_by(AuthorId) %>%
#   filter(YearsExperience == max(YearsExperience)) %>%
#   ungroup() %>%
#   mutate(censored = ifelse(Year >= end_year_duration, 1, 0)) %>%
#   select(AuthorId, career_end = YearsExperience, censored)
# 


### Add field info
author_careerbins <- author_careerbins %>%
  left_join(author_fields %>%
              select(AuthorId, main_field, field_group),
            by = c("AuthorId")) 

