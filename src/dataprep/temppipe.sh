#!/bin/bash

output_path="../../../output/"
script_path="main"
logfile_path="temp/"

python3 -m $script_path.prep_mag.prep_collab --nauthors "all" --chunksize 10000 --ncores 10  &> $logfile_path/prep_collab.log
python3 -m $script_path.prep_mag.read_collab &> $logfile_path/read_collab.log


# python3 -m main.prep_mag.prep_collab --nauthors 100
# python3 -m main.prep_mag.read_collab 

# some performance. note that larger sample -> different sample -> scale may not be 1-1 exactly
# 100_000 authors, 10_000 chunk size, 10 cores -> 10 steps: 0.77 minutes
# 100_000 authors, 20_000 chunk size, 10 cores -> 5 steps: 0.40 minutes
# 100_000 authors, 100_000 chunk size, 10 cores -> 1 step: 1.57 minutes
# why do they not take the same time?
# 200_000 authors, 20_000 chunk size, 10 cores -> 10 steps: 0.93 minutes -- but should this not take the same time? is there such a high overhead of making more cores?
# 100_000 authors, 20_000 chunk size, 5 cores -> 10 steps: 0.39 minutes -- but should this not take the same time?
 
# 200_000 authors, 40_000 chunk size, 5 cores -> 5 steps: 0.78 minutes 
# 200_000 authors, 50_000 chunk size, 4 cores -> 4 steps: 0.92 minutes
# 200_000 authors, 30_000 chunk size, 6 cores -> 4 steps: 0.86  minutes --> NOTE: some inefficiency here b/c the last core is not used to the same amount as the previous ones
# 200_000 authors, 10_000 chunk size, 10 cores -> 20 steps: 0.87  minutes

# 400_000 authors, 40_000 chunk size, 10 cores -> 5 steps: 1.57 minutes 
# 400_000 authors, 40_000 chunk size, 5 cores -> 5 steps:  1.93 minutes -- so, more cores IS better 
# 400_000 authors, 10_000 chunk size, 10 cores -> 40 steps:  1.34 minutes 

# 800_000 authors, 40_000 chunk size, 10 cores -> 20 steps: 4.49 minutes -- this now seems to hit the cpu constraint. why? and why not before?
# 800_000 authors, 30_000 chunk size, 10 cores -> XX steps: 4.58 minutes 
# 800_000 authors, 20_000 chunk size, 10 cores -> 40 steps: 4.11 minutes 
# 800_000 authors, 10_000 chunk size, 10 cores -> 80 steps: 3.87 minutes 
# 800_000 authors, 8_000 chunk size, 10 cores -> 80 steps:  3.92 minutes 

# 1_600_000 authors, 10_000 chunk size, 10 cores -> 80 steps: 8.22 minutes 
# 1_600_000 authors, 20_000 chunk size, 10 cores -> 80 steps: 8.61 minutes 
# 1_600_000 authors, 40_000 chunk size, 10 cores -> 40 steps: 9.32 minutes 
# 1_600_000 authors, 8_000 chunk size, 10 cores ->  XX steps: 7.90 minutes 
# 1_600_000 authors, 6_000 chunk size, 10 cores ->  XX steps: 7.95 minutes  
# 1_600_000 authors, 100_000 chunk size, 5 cores ->  XX steps: 16 minutes 

# 3_200_000 authors, 10_000 chunk size, 10 cores ->  XX steps: 16 minutes 
# 3_200_000 authors, 20_000 chunk size, 10 cores ->  XX steps: 16.6 minutes 
# 3_200_000 authors, 8_000 chunk size, 10 cores ->  XX steps: 15.9 minutes 

# around 10000, larger chunk sizes seem to get better than smaller ones (compare 8k and 10k)?



# is the memory shared across cores? -> this means that more cores are not necessarily better? 

# note: it does not seem to make a read-only conn? 