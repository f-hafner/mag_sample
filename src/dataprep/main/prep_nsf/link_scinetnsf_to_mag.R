# Link SciSciNet_Links_NSF table with Paper_Author_Affiliations, Authors, and NSF_Investigator 
# Keeps only those with link between NSF grant and author ID.
# only those links with a similar name (similarity >=0.8) are loaded into db
# Data downloaded and uploaded into db in: scinet_data_to_db.py in same folder

# Initialize variables for counting rows and timestamp
start_time <- Sys.time()
cat(sprintf("Started at %s \n", start_time))

packages <- c("tidyverse", "broom", "dbplyr", "RSQLite", "stringdist", "purrr", "furrr")
lapply(packages, library, character.only = TRUE)

datapath <- "/mnt/ssd/"
db_file  <- paste0(datapath, "AcademicGraph/AcademicGraph.sqlite")


con <- DBI::dbConnect(RSQLite::SQLite(), db_file)
cat("The database connection is: \n")
src_dbi(con)
cat("Connected to db...\n")

# Create table with all links between NSF-grant and authors via papers

NSF_to_Authors <- tbl(con, sql("
                  select a. PaperID, a.GrantID, b.AuthorId
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

nsf_to_authors <- nsf_to_authors %>%
  filter(!is.na(PIFullName) & !is.na(NormalizedName))
cat("Loaded dataset. \n")

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
cat("Separated names in dataset. \n")
### Compare name similarity
# Set a threshold for similarity
threshold <- 0.7

### Create function to calculate similarity and filter 

fct_similarity <- function(row) {
  mag_firstname <- row$mag_firstname
  nsf_firstname <- row$nsf_firstname

  # Calculate string distances by row using Optimal String Alignment (default)
  first_row_similarity <- stringsim(
    mag_firstname,
    nsf_firstname,
    method="osa" )
  
  
  mag_lastname <- row$mag_lastname
  nsf_lastname <- row$nsf_lastname
  
  last_row_similarity <- stringsim(
    mag_lastname,
    nsf_lastname,
    method = "osa"
  )
  
    return(data.frame(firstname_similarity = first_row_similarity, lastname_similarity = last_row_similarity))
}

# Split the data into chunks of 50,000 rows
chunk_size <- 50000
chunks <- split(nsf_to_authors, ceiling(seq_len(nrow(nsf_to_authors)) / chunk_size))

# Load the furrr package for parallel processing
plan(multisession, workers=16)

# Initialize variables for progress tracking
total_chunks <- length(chunks)
processed_chunks <- 0

# Process and save each chunk as individual CSV files
 time <- Sys.time()
 cat(sprintf(
  "Start processing %d chunks at %s \n", total_chunks,	start_time))

for (i in seq_along(chunks)) {
  chunk <- chunks[[i]]
  
  # Calculate similarity and filter rows row by row
  row_similarities <- purrr::map_df(1:nrow(chunk), ~fct_similarity(chunk[.x, ])) %>%
    mutate(id = row_number())
  
  # Filter rows that meet the threshold criteria
  chunk <- chunk %>%
    mutate(id = row_number()) %>%
    left_join(row_similarities, by = "id") %>%
    filter(
      (firstname_similarity >= threshold & lastname_similarity >= threshold) | (lastname_similarity == 1.0 & substr(mag_firstname, 1, 1) == substr(nsf_firstname, 1, 1)) ) %>%
    select(GrantID, AuthorId, Position, mag_firstname, nsf_firstname, firstname_similarity, mag_lastname, nsf_lastname, lastname_similarity) %>%
    distinct()
    
  
  # Define the output file path
  output_file <- file.path("/mnt/ssd/chunks_nsf_links", paste0("chunk_", i, ".csv"))

# Remove the output file if it exists
if (file.exists(output_file)) {
  file.remove(output_file)
}
  
  # Write the chunk to a CSV file
  write.csv(chunk, file = output_file, row.names = FALSE)
  
  # Update progress
  processed_chunks <- processed_chunks + 1
  percent_processed <- (processed_chunks / total_chunks) * 100
  elapsed_time <- as.numeric(Sys.time() - start_time)


  # Display progress information
    cat(sprintf(
      "Processed %d out of %d chunks (%.2f%%) in %.2f.\n",
      processed_chunks, total_chunks, percent_processed, elapsed_time
    ))
}

# Clean up the furrr plan
plan(NULL)

# Some info
final_elapsed_time <- Sys.time() - start_time
final_elapsed_time <- as.numeric(final_elapsed_time)

# Convert elapsed time to minutes and potentially hours

  # Display progress information
  cat(sprintf(
    "Complete. Total elapsed time: %.2f.\n",
    final_elapsed_time
  ))



# Apend tables together
# Initialize an empty data frame to store the appended data
links_nsf_mag <- data.frame()

# Loop through the file names and append the data
for (i in 1:1072) {
  # Construct the file path for each chunk
  
  # Load the CSV file
  chunk_data <- read.csv(paste0("/mnt/ssd/chunks_nsf_links/chunk_", i, ".csv"), header = TRUE, colClasses = c(GrantID = "character"))
  
  # Append the chunk data to the appended_data data frame
  links_nsf_mag <- rbind(links_nsf_mag, chunk_data)
}

# drop unnecessary variables and drop duplicates
links_nsf_mag <- links_nsf_mag %>%
  select(GrantID, AuthorId, Position, firstname_similarity, lastname_similarity) %>%
  distinct()


# Write the appended data to a single CSV file
#write.csv(links_nsf_mag, "links_nsf_mag.csv", row.names = FALSE)


# Write table to db:
dbWriteTable(con, name = "links_nsf_mag", value = links_nsf_mag, overwrite = TRUE)
cat("Uploaded to db.\n")

# close connection to db
DBI::dbDisconnect(con)
cat("Disconnected from db.\n")