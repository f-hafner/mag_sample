
# Start a jupyter lab server. Call with `source start_jupyter.sh`

eval "$(conda shell.bash hook)"
conda activate science-career-tempenv

env PYTHONPATH="$pwd"/src/dataprep/ jupyter-lab --no-browser

