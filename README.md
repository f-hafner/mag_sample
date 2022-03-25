# mag_sample
This is some sample code of my research projects. The present repository is an extract from a project that uses data from Microsoft Academic Graph (MAG) and ProQuest. 

In another project I use confidential data and everything is stored on a separate server that is not connected to the general internet. There I use more R code for data analysis than in the present repository.


## Directory structure
- `src/`: data preparation and linking; analyze the publication careers of scientists.
- `output/`: destination for tables and figures generated in `src/`. 

## Contents

### directory `src/dataprep` 
The `pipeline.sh` script calls consecutively the scripts for 
- setting up the sqlite database with MAG data
- preparing additional tables for analysis
- loading the ProQuest data into the database 
- linking records between MAG and ProQuest
- making some reports about the data and linking quality


### directory `src/analysis` 
The `pipeline.sh` script calls consecutively the scripts for
- comparing graduation trends in ProQuest data and official statistics
- assess the quality of current links 
- figures of publication dynamics over the career
- analysis of career duration and becoming an advisor later in the career 
- supporting scripts are in `setup.R` and in `helpers/`



## Notes 
See open issues for some features that are currently lacking. 