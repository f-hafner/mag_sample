# Link SciSciNet_Links_NSF table with Paper_Author_Affiliations, Authors, and NSF_Investigator 
# Keeps only those with link between NSF grant and author ID.
# only those links with a similar name (similarity >=0.8) are loaded into db
# Data downloaded and uploaded into db in: scinet_data_to_db.py in same folder

# Initialize variables for counting rows and timestamp
row_count <- 0
start_time <- Sys.time()
cat(sprintf("Started at", start_time))

packages <- c("tidyverse", "broom", "dbplyr", "RSQLite", "stringdist")
lapply(packages, library, character.only = TRUE)

datapath <- "/mnt/ssd/"
db_file  <- paste0(datapath, "AcademicGraph/AcademicGraph.sqlite")


con <- DBI::dbConnect(RSQLite::SQLite(), db_file)
cat("The database connection is: \n")
src_dbi(con)
cat("Connected to db...\n")

# Create table with all links between NSF-grant and authors via papers

NSF_to_Authors <- tbl(con, sql("
                  select a. PaperID, a.Type, a.GrantID, b.AuthorId
                        ,c.NormalizedName, d.Position, d.PIFullName
                                      from scinet_links_nsf as a
                                      inner join (
                                        select PaperId AS PaperID, AuthorId
                                        from PaperAuthorAffiliations
                                      )b
                                      using (PaperID)
                                      inner join (
                                        select AuthorId, NormalizedName
                                        from Authors
                                      ) c
                                      using (AuthorId)
                                      inner join (
                                        select GrantID, Position, PIFullName
                                        from NSF_Investigator
                               ) d
                               using (GrantID)
                               "))

nsf_to_authors <- collect(NSF_to_Authors)

# Create separate variables for first and last name for both nsf and mag names
nsf_to_authors <- nsf_to_authors %>%
  mutate(
    mag_firstname = word(NormalizedName, 1),
    mag_lastname = word(NormalizedName, -1),
    mag_middlename = ifelse(str_count(NormalizedName, "\\s+") >= 2 &
                              word(NormalizedName, 2) != word(NormalizedName, -1),
                            word(NormalizedName, 2), NA_character_)
  )


nsf_to_authors <- nsf_to_authors %>%
  mutate(
    nsf_firstname = word(PIFullName, 1),
    nsf_lastname = word(PIFullName, -1),
    nsf_middlename = ifelse(str_count(PIFullName, "\\s+") >= 2 &
                     word(PIFullName, 2) != word(PIFullName, -1),
                     word(PIFullName, 2), NA_character_)
  )



### Compare name similarity
# Set a threshold for similarity
threshold <- 0.8


### Test several distances

# Calculate string similarity for first and last names by row and add a new column
firstname_similarity <- numeric(0)
lastname_similarity <- numeric(0)

# Iterate through rows and calculate string distances for first and last names separately
cat("Start comparing names...\n")
for (i in 1:nrow(nsf_to_authors)) {
  mag_firstname <- nsf_to_authors$mag_firstname[i]
  nsf_firstname <- nsf_to_authors$nsf_firstname[i]
  
  # Calculate string distance for first name by row using Optimal String Alignment (default)
  first_row_similarity <- stringsim(
    mag_firstname,
    nsf_firstname,
    method="osa" )
  


 # Calculate string distance for last name by row
    mag_lastname <- nsf_to_authors$mag_lastname[i]
    nsf_lastname <- nsf_to_authors$nsf_lastname[i]
  
    last_row_similarity <- stringsim(
     mag_lastname,
     nsf_lastname,
     method="osa"
  )
  
  # Append the calculated distances to the results vector 
  firstname_similarity <- c(firstname_similarity, first_row_similarity)
  lastname_similarity <- c(lastname_similarity, last_row_similarity)

  # Increment row count
  row_count <- row_count + 1
  
  # Progress after each 500,000th row
  if (row_count %% 50 == 0) {
    # Calculate elapsed time
    elapsed_time <- Sys.time() - start_time 
    elapsed_time <- as.numeric(elapsed_time)
    
    # Calculate percentage of data processed
    percent_processed <- (row_count / nrow(nsf_to_authors)) * 100
    
    # Some information
    cat(sprintf(
      "Processed %d rows (%.2f%%) in %2.f minutes.\n",
      row_count, 
      percent_processed, 
      elapsed_time
    ))
  }
}

elapsed_time <- Sys.time() - start_time
elapsed_time <- as.numeric(elapsed_time)

percent_processed <- (row_count / nrow(nsf_to_authors)) * 100
cat(sprintf(
  "Processed all rows (%.2f%%) in %2.f minutes.\n",
  percent_processed, 
  elapsed_time
))

# Assign the calculated distances to a new column in data frame
nsf_to_authors$firstname_similarity <- firstname_similarity
nsf_to_authors$lastname_similarity <- lastname_similarity

# Filter observations where the names are above the threshold: threshold seemed reasonable as it allows for a single typo
similar_names <- nsf_to_authors %>%
  filter(firstname_similarity >= threshold & lastname_similarity >= threshold)

# drop unnecessary variables and drop duplicates
df <- similar_names %>%
  select(GrantID, AuthorId, Position, firstname_similarity, lastname_similarity) %>%
  distinct()

# Write table to db:
cat("Starting data upload to the database...\n")
#dbWriteTable(con, name = "links_nsf_mag2", value = df, overwrite = TRUE)
cat("Data upload to the database is complete.\n")

# Some info
final_elapsed_time <- Sys.time() - start_time
elapsed_time <- as.numeric(elapsed_time)

cat(sprintf(
  "Complete. Total time elapsed: %2.f minutes.\n",
  elapsed_time
))

# close connection to db
DBI::dbDisconnect(con)
cat("Disconnected from db.\n")