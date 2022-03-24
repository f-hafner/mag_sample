
# data_duration
# Load data for duration analysis, prepare main data frames 

# ## 1. Load data into memory 
cat("Loading data into memory... \n")

d_career <- tbl(con, "pq_authors") %>%
  select(goid, degree_year, university_id) %>% 
  inner_join(current_links, by = "goid") %>%
  inner_join(author_sample %>%
               select(AuthorId, YearLastPub, YearFirstPub),
             by = c("AuthorId")) %>%
  inner_join(author_info %>%
               select(AuthorId, gender, censored), 
             by = "AuthorId") %>%
  filter(degree_year >= degree_year_start & degree_year <= degree_year_end) # this reduces the sample by almost half... should we omit the restriction?


papers_career <- tbl(con, "PaperAuthorUnique") %>%
  inner_join(tbl(con, "Papers") %>%
               select(PaperId, Year, DocType, CitationCount),
             by = "PaperId") %>%
  inner_join(tbl(con, "paper_outcomes") %>%
               select(PaperId, CitationCount_y5),
             by = "PaperId") %>% 
  inner_join(d_career %>%
               select(AuthorId, degree_year),
             by = "AuthorId") %>%
  filter(DocType %in% c("Journal", "Conference", "Book", "BookChapter")) %>%
  inner_join(tbl(con, "PaperMainFieldsOfStudy") %>%
               select(PaperId, Field1, Field0),
             by = "PaperId")


d_career <- d_career %>%
  left_join(author_fields,
            by = "AuthorId") %>% 
  collect()

advisor_links <- collect(advisor_links)

papers_career <- collect(papers_career)


# ## 2. Further processing 

cat("Preparing main dataframe, adjusting output measures by year and field, adjusted author output... \n")

# ### a) main data frame for duration analysis
d_career <- d_career %>%
  filter(!is.na(gender)) %>% # NAs b/c probability female unclear or missing
  select(-goid) %>%
  mutate(career_length = YearLastPub - degree_year,
         career_length_huang = YearLastPub - YearFirstPub) %>% 
  mutate(female = ifelse(gender == "Female", 1, 0),
         field_f = factor(first_field_id),
         main_field_f = factor(main_field),
         # uni_f = factor(university_id),
         gradyear_f = factor(degree_year),
         # more detailed FEs (see note below)
         uni_field_f = factor(paste0(university_id, "_", main_field)), # note: the code below still uses uni_f, which used to be uni x main field!!
         uni_f = factor(university_id),
         gradyear_field_f = factor(paste0(degree_year, "_", main_field)),
         fld = factor(field_group)
  ) %>%
  select(-university_id, -YearFirstPub, -YearLastPub, -main_field, -first_field_id)

# ### b) Adjust publication output, following Radicchi et al 
  # Adjust by year vs adjsut by field and year 

  # Note: Heckman/Moktan calculates the conditional expectation with a regression
  # 2 problems: regression may yield negative predicted values; cannot multiply by conditional expectation

cat("\t Dropping papers with missing field1 \n \t")
print(table(is.na(papers_career$Field1)))

papers_career <- papers_career %>%
  filter(!is.na(Field1)) %>%
  # filter(DocType == "Journal") %>%
  mutate(cit_measure = CitationCount_y5) %>%
  mutate(cit_measure = cit_measure) 

# normalize by year-field and by year 
agg_year_field <- papers_career %>%
  group_by(Year, Field1) %>%
  summarise(ci_year_field = mean(cit_measure),
            .groups = "drop")

agg_year <- papers_career %>%
  group_by(Year) %>%
  summarise(ci_year = mean(cit_measure),
            .groups = "drop")

papers_career <- papers_career %>%
  left_join(agg_year, by = "Year") %>%
  left_join(agg_year_field, by = c("Year", "Field1"))

rm(agg_year, agg_year_field)


# adjusted measure of output  
interval_length_afterstart <- 3

author_output_adjusted <- papers_career %>%
  mutate(exper = ifelse(
    Year <= degree_year + 5, 
    5,
    5 + interval_length_afterstart* (floor((Year - degree_year) / interval_length_afterstart)-1)
  )) %>%
  mutate(cit_adj = cit_measure / ci_year_field) %>%
  select(AuthorId, PaperId, Year, exper, cit_adj)

cat("dropping author-paper observations where cit_adj is NA. yes-no:", table(is.na(author_output_adjusted$cit_adj)))
author_output_adjusted <- author_output_adjusted %>%
  filter(!is.na(cit_adj))

# calculate the cumulated output measures: for each exper,
  # keep the current exper and all that are smaller than exper 
  # Note that this already creates the "panel" dimension. Need to drop the data with exper > career length
author_output_adjusted <- map(.x = unique(author_output_adjusted$exper),
    .f = ~author_output_adjusted %>%
      filter(exper <= .x) %>%
      group_by(AuthorId) %>%
      summarise(cites_q25 = quantile(cit_adj, probs = 0.25),
                cites_q50 = quantile(cit_adj, probs = 0.5),
                cites_q75 = quantile(cit_adj, probs = 0.75),
                n_cites = sum(cit_adj),
                eucl_idx = sqrt(sum(cit_adj^2)),
                n_papers = n(),
                .groups = "drop") %>% 
      mutate(avg_cites = n_cites / n_papers,
             exper = .x) 
      ) %>%
  bind_rows()

# ## 3. Sample definitions

cat("Defining samples... \n")


output_vars <- c("n_cites", "n_papers", "avg_cites", "eucl_idx")

# define samples of authors 
analysis_samples <- list(
  fullsample = d_career %>%
    filter(career_length >= 0) %>%
    select(AuthorId, main_field_f, career_length), 
  early_survivors = d_career %>%
    filter(career_length >= min_careerlength) %>%
    select(AuthorId, main_field_f, career_length)
)

# for decomposition: do not take logs, do not rescale 
analysis_samples_decomp <- map(
  .x = analysis_samples,
  .f = ~.x %>%
    left_join(author_output_adjusted, by = "AuthorId") %>%
    group_by(exper) %>%
    mutate(across(.cols = all_of(output_vars),
                  .fns = ~prep_xvars(.x, transform = NULL, rescale = F, recenter = F)
    )
    ) %>%
    ungroup() %>%
    filter(!is.na(n_cites)) 
)


analysis_samples <- map(
  .x = analysis_samples,
  .f = ~.x %>%
    left_join(author_output_adjusted, by = "AuthorId") %>%
    # drop periods after the person dies: -1 so that the next period after death does not start anymore
    filter(exper <= career_length + (interval_length_afterstart - 1)) %>%
    group_by(exper) %>%
    mutate(across(.cols = all_of(output_vars),
                  .fns = ~prep_xvars(.x, transform = "asinh", rescale = F, recenter = T)
    )
    ) %>%
    ungroup() %>%
    filter(!is.na(n_cites)) %>% 
    group_by(main_field_f, exper) %>% 
    mutate(qnt_papers = define_rank(n_papers),
           qnt_cites = define_rank(n_cites),
           qnt_avg_cites = define_rank(avg_cites),
           qnt_idx = define_rank(eucl_idx)) %>%
    ungroup() %>%
    select(-main_field_f) %>%
    left_join(author_output_adjusted %>%
                select(AuthorId, exper, 
                       n_papers_lvl = n_papers,
                       eucl_idx_lvl = eucl_idx,
                       n_cites_lvl = n_cites),
              by = c("AuthorId", "exper"))
)

## 4. Make data frames for analysis

cat("Data frames for analysis... \n")

# df for kaplan-meier
df_kaplanmeier <- d_career %>%
  inner_join(analysis_samples$fullsample %>%
               filter(exper == 5) %>%
               select(AuthorId, eucl_idx, n_papers, n_papers_lvl),
             by = "AuthorId") %>%
  mutate(career_length = ifelse(censored == 1, end_year_duration - degree_year, career_length),
         status = 1 - censored)


# df for career length returns 
df_returns <- d_career %>%
  inner_join(analysis_samples$early_survivors %>%
               filter(exper == 5) %>%
               select(AuthorId, n_cites, n_papers, avg_cites, eucl_idx,
                      cites_q25, cites_q50, cites_q75,
                      qnt_papers, qnt_cites, qnt_avg_cites, qnt_idx,
                      ends_with("lvl") ),
             by = "AuthorId") %>%
  filter(!is.na(n_cites)) 

stopifnot(n_distinct(df_returns$AuthorId) == nrow(df_returns))

# df for duration analysis (with time-varying covariates)
  # add the lagged output measures, but have a df with all the active years 

df_duration <- df_returns %>%
  select(AuthorId, female, censored, fld, career_length, 
         field_f, main_field_f, gradyear_f)
# expand this to all years from 5 to end of career 
dk <- expand.grid(AuthorId = unique(df_duration$AuthorId),
                  experience = 5:(max(df_returns$career_length)))
df_duration <- dk %>%
  left_join(df_duration, by = "AuthorId") %>%
  mutate(exper = ifelse(
    experience <= 5, 
    5,
    5 + interval_length_afterstart* (floor(experience / interval_length_afterstart)-1)
  )) %>%
  # only need the observations at the end of each period
  filter(experience == exper)


# join output: contemporaneous for exper = 5 (no one drops out there), 1-period lagged for exper >=8
df_duration <- bind_rows(
  df_duration %>%  
    filter(exper == 5) %>%
    inner_join(analysis_samples$early_survivors %>%
                       select(AuthorId, exper, n_cites, n_papers, avg_cites, eucl_idx,
                              cites_q25, cites_q50, cites_q75,
                              qnt_papers, qnt_cites, qnt_avg_cites, qnt_idx),
                     by = c("AuthorId", "exper")),
  df_duration %>%  
    filter(exper > 5) %>%
    inner_join(analysis_samples$early_survivors %>%
                 mutate(exper = exper - interval_length_afterstart) %>%
                 select(AuthorId, exper, n_cites, n_papers, avg_cites, eucl_idx,
                        cites_q25, cites_q50, cites_q75,
                        qnt_papers, qnt_cites, qnt_avg_cites, qnt_idx),
               by = c("AuthorId", "exper"))
)

# keep the spells up until the last one: the one during which the person dies 
df_duration <- df_duration %>%
  mutate(career_end = ifelse(
    career_length <= 5, 
    5,
    5 + interval_length_afterstart* (floor(career_length / interval_length_afterstart)-1)
  )) %>%
  filter(exper <= career_end) %>%
  select(-career_end)

# re-code the last period in which the person disappears: replace exper with career_length
df_duration <- df_duration %>% 
  group_by(AuthorId) %>%
  mutate(last_exper = max(exper)) %>%
  ungroup() %>%
  mutate(exper = ifelse(exper == last_exper, career_length, exper)) %>%
  select(-last_exper, -experience)


# df for decomposition
df_decomp <- d_career %>%
  inner_join(analysis_samples_decomp$early_survivors %>%
               select(AuthorId, n_cites, n_papers, avg_cites, exper),
             by = "AuthorId") %>%
  filter(!is.na(n_cites))

# filter observations after end of career 
df_decomp <- df_decomp %>%
  mutate(career_end = ifelse(
    career_length <= 5, 
    5,
    5 + interval_length_afterstart* (floor(career_length / interval_length_afterstart)-1)
  )) %>%
  filter(exper <= career_end) %>%
  select(-career_end)



cat("Decomposing quality and quantity")

df_decomp <- df_decomp %>%
  group_by(gender, exper, main_field_f) %>%
  summarise(CovQualQuant = cov(x = n_papers, y = avg_cites),
            CitationPerPaper = mean(avg_cites),
            PaperCount = mean(n_papers),
            TotalForwardCitations = mean(n_cites),
            AuthorCount = n_distinct(AuthorId),
            .groups = "drop") 


# ## raw gaps for each measure
d_gap <- df_decomp %>%
  select(-AuthorCount) %>%
  gather(key = measure, value = val,
         -gender, -exper, -main_field_f) %>%
  spread(key = gender, value = val) 

# ## gender-specific averages of PaperCount and CitationPerPaper
# for decomp
means_parts <- df_decomp %>%
  select(-CovQualQuant) %>%
  gather(key = measure, value = val,
         -gender, -exper, -main_field_f) %>%
  mutate(measure = paste0(measure, "_", gender)) %>%
  select(-gender) %>%
  spread(key = measure, value = val)

df_decomp <- d_gap %>%
  left_join(means_parts, by = c("exper", "main_field_f")) %>%
  # need to restrict at least to cells with >1 authors, but probably better to have
  # (an even) higher threshold?
  filter(AuthorCount_Female > 5 & AuthorCount_Male > 5)

# ## decomposition
# change weights for CitationPerPaper and PaperCount
df_decomp <- df_decomp %>%
  mutate(gap = (Female - Male) / TotalForwardCitations_Male,
         gap = case_when(
           measure == "TotalForwardCitations" ~ gap,
           measure == "CovQualQuant" ~ gap,
           measure == "CitationPerPaper" ~ PaperCount_Male * gap,
           measure == "PaperCount" ~ CitationPerPaper_Female * gap
         )) %>%
  select(exper, measure, main_field_f,
         gap, starts_with("AuthorCount")) %>%
  mutate(AuthorCount_Total = AuthorCount_Female + AuthorCount_Male)


df_decomp <- df_decomp %>% 
  left_join(df_decomp %>%
              filter(measure == "TotalForwardCitations") %>%
              select(exper, main_field_f, gap_total = gap),
            by = c("exper", "main_field_f"))





