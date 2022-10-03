
# Read and clean the data on U.S. graduates from NCES

rm(list = ls())

# This script uses the data from NCES to make the tables:
  # graduate_counts.csv: number of graduates by field 0 and year
  # postgrad_us_counts.csv: number of graduates with postgraduate commitment in US by year and field 1
  # postgrad_us_sectors.csv: sector (share of total with US commitment) of graduates by year and field 1
  # fields.csv: field ids to names 
  # parent_field.csv: correspondence from field 1 to 0. Note that not all of them directly correspond to MAG fields.



if (interactive()) {
  setwd("~/projects/mag_sample/src/dataprep")
}

packages <- c("tidyverse", "readxl", "data.table")

lapply(packages, library, character.only = TRUE)

# ## 0. Setup

datapath <- "/mnt/ssd/NCES_NSF/"
rawpath <- paste0(datapath, "raw/")
processedpath <- paste0(datapath, "processed/")

file_tab46_suppl <- paste0(rawpath, "nsf_table46_supplement.xlsx")
file_graduate_counts <- paste0(rawpath, "nces_graduates_field_gender.csv")
file_graduate_counts_field_uni <- paste0(rawpath, "ncses_graduates_field_uni.csv")

source("helpers/setup_nces.R")


# ## 1. Number of graduates by field, year and gender
col_names <- c("year", "se_fld", "broad_fld", "det_fld",
               "male", "female", "other", "last")
data_graduates <- fread(file_graduate_counts,
                        skip = 9, nrows = 2165, # need to provide end by hand...
                        col.names = col_names)

data_graduates <- data_graduates %>%
    select(-last) %>%
    mutate(across(all_of(c("se_fld", "broad_fld", "det_fld")),
                  ~case_when(
                      .x == "Total for selected values" ~ "total",
                      .x == "Detailed field suppressed for temporary visa holders" ~ "temp_visa",
                      TRUE ~ tolower(.x)
                  )))


data_graduates <- data_graduates %>%
    filter(det_fld == "total"
            & broad_fld != "total"
            & se_fld != "total") %>%
    filter(broad_fld != "education") # they are not in the MAG fields



data_graduates <- data_graduates %>%
    select(-det_fld) %>%
    gather(key = gender, value = nb, male:other) %>%
    mutate(nb = ifelse(nb == "-", 0, nb),
           nb = as.numeric(gsub(",", "", nb)))

data_graduates <- data_graduates %>%
    mutate(fld = case_when(
        broad_fld == "mathematics and computer sciences" ~ "math_compsc",
        broad_fld == "psychology and social sciences" ~ "socsci",
        broad_fld == "physical sciences and earth sciences" ~ "phys_earth",
        broad_fld == "engineering" ~ "engn",
        broad_fld == "humanities and arts" ~ "humanities",
        broad_fld == "life sciences" ~ "life_sc",
        broad_fld == "other non-s&e" ~ "other"
    ))


# ## 2. Post-graduate commitments in U.S.

df_postgrad <- lapply(names(inputs_tab46_suppl), function(x) {
  specs <- inputs_tab46_suppl[[x]]

  df <- read_field(fn = file_tab46_suppl, 
                   specs = specs) %>%
    mutate(field = x)
  
  return(df)
}) %>%
  bind_rows() 

df_postgrad <- df_postgrad %>%
  gather(key = gender, value = val, male:female) %>%
  mutate(val = ifelse(var == "total_count", val, val / 100))


# note: for now we have to get by with the broad fields because we do not have the detailed ones from the online tool? or could we ask for it as well?
  # this is because the "detailed field was suppressed for temp visa holders". we could ask for a special extract as well 
  # for now we can do it for phys, cs/math and eng where we have all fields covered by the linking 


# ## 3. Prepare output tables

field_lvl_0 <- data_graduates %>%
  select(fullname = broad_fld, shortname = fld) %>%
  filter(!duplicated(shortname)) %>%
  mutate(field_id = 1:n()) %>%
  select(field_id, fullname, shortname)

data_graduates <- data_graduates %>%
  left_join(field_lvl_0 %>%
              select(shortname, field_id),
            by = c("fld" = "shortname")) %>%
  select(-broad_fld, -fld, -se_fld) %>%
  select(field_id, year, gender, nb)

# note: for engineering, we will have two field ids, one for level 0 and 1
field_lvl_1 <- df_postgrad %>%
  select(field) %>%
  filter(!duplicated(field)) %>%
  mutate(field_id = 1:n(),
         field_id = field_id + max(field_lvl_0$field_id)) %>%
  mutate(fullname = case_when(
    field == "agri" ~ "agricultural sciences and natural resources",
    field == "biology" ~ "biological and biomedical sciences",
    field == "health" ~ "health sciences",
    field == "chem" ~ "chemistry",
    field == "geo" ~ "geosciences, atmospheric and ocean sciences",
    field == "phys" ~ "physics and astronomy",
    field == "cs" ~ "computer and information sciences",
    field == "math" ~ "mathematics and statistics",
    field == "engn" ~ "engineering",
    field == "psych" ~ "psychology",
    field == "econ" ~ "economics",
    field == "polisci" ~ "political science and government",
    field == "soc" ~ "sociology",
    field == "other_socsci" ~ "other social sciences",
    field == "educ" ~ "education",
    field == "hum_art" ~ "humanities and art",
    field == "other" ~ "other"
  ))  %>%
  mutate(parent_field_name = case_when(
    field %in% c("agri", "biology", "health") ~ "life_sc",
    field %in% c("chem", "geo", "phys") ~ "phys_earth",
    field %in% c("cs", "math") ~ "math_compsc",
    field %in% c("engn") ~ "engn",
    field %in% c("psych", "polisci", "soc", "other_socsci", "econ") ~ "socsci", # NOTE: this differs from Ceci et al 
    field %in% c("hum_art") ~ "humanities",
    field %in% c("other", "educ") ~ "other" # this may not be correct; better ignore for analysis
  )) %>%
  rename(shortname = field) %>%
  select(field_id, fullname, shortname, parent_field_name)

parent_fields <- field_lvl_1 %>%
  left_join(field_lvl_0 %>%
              select(parent_field_id = field_id,
                     shortname),
            by = c("parent_field_name" = "shortname")) %>%
  select(field_id, parent_field_id)

field_lvl_1 <- field_lvl_1 %>%
  select(-parent_field_name)


df_postgrad <- df_postgrad %>%
  left_join(field_lvl_1 %>%
              select(shortname, field_id),
            by = c("field" = "shortname")) %>%
  select(-field)

# separate into counts and shares
postgrad_us_counts <- df_postgrad %>%
  filter(var == "total_count") %>%
  select(field_id, year, gender, nb = val)

postgrad_us_sectors <- df_postgrad %>%
  filter(var != "total_count") %>%
  select(field_id, year, gender, sector = var, share = val) %>%
  spread(key = sector, value = share) %>%
  mutate(other = 1 - academia - business - government) %>%
  gather(key = sector, value = share, 
         -field_id, -year, -gender)

# ## 4. Number of graduates by detailed field, year and institution
  # (ipeds unitid = carnegie unitid)
col_names <- c("se_fld", "unitid", seq(2020, 1957, by = -1)) # for some reason there is another empty column

d_uni_field <- fread(file_graduate_counts_field_uni,
                     skip = 10, 
                     col.names = col_names)
# make sure the end is read correctly
stopifnot(
  d_uni_field[nrow(d_uni_field), "unitid" ] == "221999"
)
d_uni_field <- d_uni_field %>%
  filter(unitid != "Total for selected values") %>%
  pivot_longer(names_to = "year",
               values_to = "nb",
               cols = all_of(col_names[3:length(col_names)]) ) %>%
  mutate(nb = ifelse(nb == "-", "0", nb)) %>%
  mutate(across(all_of(c("unitid", "year", "nb")),
                ~as.numeric(.))
         ) %>%
  filter(year > 1957) %>%
  mutate(se_fld = tolower(se_fld))

# they have their own classification separate from the previous data
  # aggregate at field level 0
humanities <- c("communication", "foreign languages and literature",
  "letters", "other humanities and arts"  )

other_socsi <- c("anthropology", "other social sciences", "non-s&e fields not elsewhere classified")


cw_fields <- tibble(
  ncses = c("agricultural sciences and natural resources",
            "biological and biomedical sciences",
            "business management and administration",
            "chemistry",
            "computer and information sciences",
            "economics",
            "engineering",
            "mathematics and statistics",
            "history" ,
            "psychology" ,
            "sociology" ,
            "political science and government",
            "physics and astronomy",
            "geosciences, atmospheric sciences, and ocean sciences",
            "health sciences"  
            ),
  mag = c("environmental science", 
          "biology",
          "business",
          "chemistry",
          "computer science",
          "economics",
          "engineering",
          "mathematics",
          "history" ,
          "psychology" ,
          "sociology",
          "political science",
          "physics",
          "geology", # this is probably wrong
          "health sciences"
          )
)

d_uni_field <- d_uni_field %>%
  filter(!grepl("education", se_fld)) %>%
  mutate(se_fld = ifelse(grepl("engineering", se_fld), "engineering", se_fld)) %>%
  group_by(se_fld, unitid, year) %>%
  summarise(nb = sum(nb), .groups = "drop") %>%
  left_join(cw_fields,
            by = c("se_fld" = "ncses")) %>%
  mutate(mag = case_when(
    se_fld %in% humanities ~ "humanities",
    se_fld %in% other_socsi ~ "other socsci",
    TRUE ~ mag
  )) %>% 
  rename(fieldname0_mag = mag) %>%
  group_by(unitid, year, fieldname0_mag) %>%
  summarise(nb = sum(nb),
            .groups = "drop")

# ## 5. Save 

df_fields <- bind_rows(field_lvl_0 %>% mutate(level = 0), 
                       field_lvl_1 %>% mutate(level = 1))


out <- list(
  graduate_counts = data_graduates,
  postgrad_us_counts = postgrad_us_counts,
  postgrad_us_sectors = postgrad_us_sectors,
  fields = df_fields,
  parent_fields = parent_fields,
  counts_uni_field0 = d_uni_field
)

lapply(names(out), function(x) {
  write.csv(out[[x]], file = paste0(processedpath, x, ".csv"),
            row.names = FALSE)
})

cat("Done. \n")














