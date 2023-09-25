# Link SciSciNet_Links_NSF table with Paper_Author_Affiliations, Authors, and NSF_Investigator 
# Keeps only those with link between NSF grant and author ID.
# Data downloaded and uploaded into db in: scinet_data_to_db.py in same folder


# Note: Not sure if calculating string distance now works correctly
 


packages <- c("tidyverse", "broom", "dbplyr", "RSQLite", "stringdist")
lapply(packages, library, character.only = TRUE)

datapath <- "/mnt/ssd/"
db_file  <- paste0(datapath, "AcademicGraph/AcademicGraph.sqlite")
#sciscinet_path <- paste0(datapath,"sciscinet_data/")


#filepath_nsf=paste0(sciscinet_path,"SciSciNet_Link_NSF.tsv")

con <- DBI::dbConnect(RSQLite::SQLite(), db_file)
cat("The database connection is: \n")
src_dbi(con)

# Create table with all links between NSF-grant and authors via papers 

NSF_to_Authors <- tbl(con, sql("
                  select a. PaperID, a.Type, a.GrantID, b.AuthorId, b.OriginalAuthor 
                        ,c.NormalizedName, Position, FirstName, LastName
                                      from scinet_links_nsf as a
                                      inner join (
                                        select PaperId AS PaperID, AuthorId, OriginalAuthor
                                        from PaperAuthorAffiliations 
                                      )b 
                                      using (PaperID)
                                      inner join (
                                        select AuthorId, NormalizedName
                                        from Authors
                                      ) c
                                      using (AuthorId)
                                      inner join (
                                        select GrantID, Position, FirstName, LastName
                                        from NSF_Investigator
                               ) d 
                               using (GrantID)
                               "))

nsf_to_authors <- collect(NSF_to_Authors)

# Create a variable with the full name from mag 
nsf_to_authors$mag_name <- paste(nsf_to_authors$FirstName, nsf_to_authors$LastName, sep = " ")

## Still running, not sure if running correctly from here

### Compare name similarity
# Set a threshold for similarity
threshold <- 0.8

# Calculate string similarity for each row and add a new column
name_similarity <- numeric(0)


# Iterate through rows and calculate string distances
for (i in 1:nrow(nsf_to_authors)) {
  mag_name <- nsf_to_authors$mag_name[i]
  NormalizedName <- nsf_to_authors$NormalizedName[i]
  
  # Calculate string distance for this row
  row_similarity <- stringsim(
    mag_name,
    NormalizedName
  )
  
  # Append the calculated distance to the results vector
  name_similarity <- c(name_similarity, row_similarity)
}

# Assign the calculated distances to a new column in data frame
nsf_to_authors$name_similarity <- name_similarity

# Filter observations where the names are above the threshold
similar_names <- nsf_to_authors %>%
  filter(name_similarity >= threshold)

# drop unnecessary variables
df <- similar_names %>%
  select(GrantID, AuthorId, Position) %>% 
  distinct()

# Write table to db: 
dbWriteTable(con, name = "links_nsf_mag", value = df, overwrite = TRUE)

# close connection to db
DBI::dbDisconnect(con)