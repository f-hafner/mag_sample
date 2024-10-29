
# Start a jupyter lab server on remote.
# 1. Call with `source start_jupyter.sh`
# 2. On local machine, use port forwarding as described here: https://vmascagn.web.cern.ch/vmascagn/LABO_2020/jupyter_remote.html
# 3. Click on the link with localhost in it, and jupyter lab should start.

eval "$(conda shell.bash hook)"
conda activate science-career-tempenv

repo_dir=$(pwd)
echo "$repo_dir"/src/dataprep
env PYTHONPATH="$repo_dir"/src/dataprep/ jupyter-lab --no-browser


