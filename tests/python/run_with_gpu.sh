#!/bin/bash

zip dep.zip *.py

/opt/spark/bin/spark-submit --master "local[*]" \
--executor-memory 5g \
--driver-memory 5g \
--name GPU_Benchmark \
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
run_benchmark.py  --outputfile "test_gpu_large" --clearcache --ntest 5 large "hdfs://localhost:9000/user/william/data/large-sample-csv-10_9" --list "q1"

/opt/hadoop/bin/hadoop dfs -rm -r -f temp
# --deploy-mode cluster \
