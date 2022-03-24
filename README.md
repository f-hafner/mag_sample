# mag_sample
This is some sample code of my research projects. I have worked on two main projects in the past.
The first uses uses data from Microsoft Academic Graph (MAG) and ProQuest. The present repo is an extract of this project.
The second project uses confidential data and everything is stored on a separate server which I can access only with a vpn.
I use more R code for data analysis than in the present repo.


## Directory structure
- `src/`: data preparation and linking.
- `analysis/`: analyze the publication careers of scientists.

## Contents

### directory `src` 
The `pipeline.sh` script calls consecutively the scripts for 
- setting up the sqlite database with MAG data
- preparing additional tables for analysis
- loading the ProQuest data into the database 
- linking records between MAG and ProQuest
- making some reports about the data and linking quality


### directory `analysis` 
The `pipeline.sh` script calls consecutively the scripts for
- comparing graduation trends in ProQuest data and official statistics
- assess the quality of current links 
- figures of publication dynamics over the career
- analysis of career duration and becoming an advisor later in the career 
- supporting scripts are in `setup.R` and in `helpers/`



## Notes 
See open issues for some features that are currently lacking. 