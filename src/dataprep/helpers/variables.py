# paths to data 
rawdatapath = "/mnt/hdd/mag/2021-05-24/"
datapath = "/mnt/ssd/"
databasepath = datapath + "AcademicGraph/"
db_file = f"{databasepath}AcademicGraph.sqlite" 

# DocTypes to keep
keep_doctypes = ("Journal", "Book", "BookChapter", "Conference")
insert_questionmark_doctypes = ",".join(["?" for i in range(len(keep_doctypes))])

# DocTypes for measuring citations
keep_doctypes_citations = ("Journal", "Book", "BookChapter", "Conference", "Thesis")
insert_questionmark_doctypes_citations = ",".join(["?" for i in range(len(keep_doctypes_citations))])

# File locations in MAG
mag_file_locations = {
  'Authors': 'mag/',
  'FieldsOfStudy': 'advanced/',
  'PaperFieldsOfStudy': 'advanced/',
  'FieldOfStudyChildren': 'advanced/',
  'PaperAuthorAffiliations': 'mag/',
  'Papers': 'mag/',
  'PaperReferences': 'mag/',
  'Affiliations': 'mag/'
}
