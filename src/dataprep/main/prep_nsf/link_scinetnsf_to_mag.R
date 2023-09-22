# Link SciSciNet_Links_NSF table with Paper_Author_Affiliations, Authors, and NSF_Investigator 
# Keeps only those with link between NSF grant and author ID.
# Data downloaded and uploaded into db in: scinet_data_to_db.py in same folder

packages <- c("tidyverse", "broom", "dbplyr", "RSQLite", "ggplot2", "stringdist", "DBI")
lapply(packages, library, character.only = TRUE)

datapath <- "/mnt/ssd/"
db_file  <- paste0(datapath, "AcademicGraph/AcademicGraph.sqlite")
sciscinet_path <- paste0(datapath,"sciscinet_data/")


#filepath_nsf=paste0(sciscinet_path,"SciSciNet_Link_NSF.tsv")

con <- DBI::dbConnect(RSQLite::SQLite(), db_file)
cat("The database connection is: \n")
src_dbi(con)

# Create table with all links between NSF-grant and authors via papers 

NSF_to_Authors <- tbl(con, sql("
                  select a. PaperID, a.Type, a.GrantID, b.AuthorId, b.OriginalAuthor 
                        ,c.NormalizedName, Position, FirstName, LastName
                                      from scinet as a
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

# Split the "NormalizedName" column into "nsf_firstname" and "nsf_lastname" columns
nsf_to_authors <- nsf_to_authors %>%
  separate(NormalizedName, into = c("nsf_firstname", "nsf_lastname"), sep = " ", extra = "merge")

nsf_author_links <- subset(nsf_to_authors, select = -c(OriginalAuthor, NormalizedName, Type, PaperID)) %>%
  mutate(name_similarity = stringdist::stringdistmatrix(paste(nsf_firstname, nsf_lastname, sep = " "), paste(FirstName, LastName, sep = " ")))

# Set a threshold for similarity (e.g., 0.8 means 80% similarity)
threshold <- 0.8

# Filter observations where the names are similar or above the threshold
similar_names <- nsf_author_links %>%
  filter(name_similarity >= threshold)

DBI::dbDisconnect(con)