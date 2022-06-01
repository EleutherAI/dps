#!/bin/bash

set -e

BASEDIR=$(dirname $0)
BINDIR=$BASEDIR/bin

source ~/.bashrc
conda activate ./environment
pip uninstall -y dps
python $BASEDIR/setup.py install

if [ -f "$BINDIR/pyspark_conda_env.tar.gz" ]; then
  rm $BINDIR/pyspark_conda_env.tar.gz
fi

if [[ "$SKIP_CONDA_PACK" != "1" ]]; then
  conda pack -p ./environment -o $BINDIR/.pyspark_conda_env.tar.gz
  mv $BINDIR/.pyspark_conda_env.tar.gz $BINDIR/pyspark_conda_env.tar.gz
fi

conda deactivate

chmod 755 $BINDIR/pyspark_conda_env.tar.gz

