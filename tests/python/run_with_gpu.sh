#!/bin/bash

zip dep.zip *.py

/opt/spark/bin/spark-submit --master yarn \
--name GPU_Benchmark \
--deploy-mode cluster \
--py-files dep.zip \
--jars 'rapids.jar,cudf.jar' \
--num-executors=1 --executor-cores=1 --executor-memory=16G \
--conf spark.plugins=com.nvidia.spark.SQLPlugin \
--conf spark.rapids.sql.incompatibleOps.enabled=true \
--conf spark.rapids.sql.explain=ALL \
--conf spark.rapids.sql.csv.read.float.enabled=true \
--conf spark.rapids.sql.csv.read.integer.enabled=true \
--conf spark.rapids.sql.variableFloatAgg.enabled=true \
--conf spark.sql.cache.serializer=com.nvidia.spark.rapids.shims.spark311.ParquetCachedBatchSerializer \
--conf spark.rapids.sql.exec.CollectLimitExec=true \
--conf spark.rapids.sql.csv.read.double.enabled=true \
--conf spark.rapids.sql.csv.read.long.enabled=true \
--conf spark.rapids.sql.castFloatToString.enabled=true \
run_benchmark.py  --outputfile "test_gpu_nyc_taxi" --ntest 3 nycdata --list "q1,q2"

# /opt/hadoop/bin/hadoop dfs -rm -r -f temp
