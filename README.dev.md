# Developer documentation 

This is a work in progress.

#### Running scripts 
An individual file should be run from `src/dataprep/`, and any new file needs to be integrated in the overall `pipeline.sh`. That is 

```bash
cd src/dataprep
conda activate science-career-tempenv 
python -m path.to.script # -- note that ".py" is not here, and we use "." instead of "/". 

```

When doing so, it should not be necessary to `sys.path.append`.