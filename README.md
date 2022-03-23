# mag_sample
This is some sample code from a private repo that uses data from Microsoft Academic Graph (MAG). 
See open issues for some features that are currently lacking. 


## Directory structure
- `src/`: data preparation and linking.
- `analysis/`: analyze the publication careers of scientists.

## Contents

### `src` 
The `pipeline.sh` script calls consecutively the scripts for 
- setting up the sqlite database with MAG data
- preparing additional tables for analysis
- loading the ProQuest data into the database 
- linking records between MAG and ProQuest
- making some reports about the data and linking quality


### `analysis` 
The `pipeline.sh` script calls consecutively the scripts for
- comparing graduation trends in ProQuest data and official statistics
- assess the quality of current links 
- figures of publication dynamics over the career
- supporting scripts are in `setup.R` and in `helpers/`

