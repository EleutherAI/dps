#!/bin/bash

set -e

BINDIR=$(dirname $0)
cd $BINDIR/..

if [[ "$1" == "-h" ]]; then
  echo 'Usage: ./bin/spark-submit.sh <job name> [<job arguments> ...]'
  exit 1
fi

export PYSPARK_DRIVER_PYTHON=./environment/bin/python3
export PYSPARK_PYTHON=./environment/bin/python3

spark-submit --archives $BINDIR/pyspark_conda_env.tar.gz#environment\
  --conf spark.shuffle.service.enabled=true\
  --conf spark.dynamicAllocation.enabled=true\
  --conf spark.dynamicAllocation.minExecutors=5\
  --conf spark.dynamicAllocation.maxExecutors=50\
  --conf spark.dynamicAllocation.initialExecutors=5\
  --conf spark.executor.memory=16g\
  --conf spark.driver.memory=16g\
  --conf spark.yarn.executor.memoryOverhead=8g\
  --conf spark.driver.maxResultSize=4g\
  --conf spark.hadoop.hive.metastore.uris= \
  --master yarn-cluster\
  --queue dev\
  $BINDIR/sparkapp.py $@

exit $?
