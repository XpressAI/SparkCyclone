#!/bin/bash

zip dep.zip *.py

/opt/spark/bin/spark-submit --master yarn \
--name GPU_Benchmark \
--deploy-mode cluster \
--py-files dep.zip \
--jars 'rapids.jar,cudf.jar' \
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
run_benchmark.py  --outputfile "test_gpu_nyc_taxi" --clearcache --ntest 5 nycdata --list "q1,q2,q3,q4,q5"

/opt/hadoop/bin/hadoop dfs -rm -r -f temp
