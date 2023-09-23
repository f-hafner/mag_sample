# mag_sample

The present repository is an extract from a project that uses data from Microsoft Academic Graph (MAG) and ProQuest. The programs work on a remote computer that has the data stored outside of the repository. The path to the data is defined in `src/analysis/setup.R` and `src/dataprep/helpers/variables.py`. 


## Directory structure
- `src/`: data preparation and linking; analyze the publication careers of scientists.
- `output/`: destination for tables and figures generated in `src/`. 
- `snapshots/`: contains files to reproduce the environments (for now, `yml` files for conda).

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
- The python helpers are stored in a different location and we need to come up with a clean and simple way to make them accessible across the repository.
- There are some redudancies between the Rscripts in `analysis` and `dataprep` because the scripts come originally from two different repositories.
