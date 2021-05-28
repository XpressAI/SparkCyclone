#!/bin/bash

/opt/spark/bin/spark-submit --master yarn \
--deploy-mode cluster \
--py-files dep.zip \
--jars 'rapids.jar,cudf.jar' \
--conf spark.plugins=com.nvidia.spark.SQLPlugin \
--conf spark.rapids.sql.incompatibleOps.enabled=true \
--conf spark.rapids.sql.explain=ALL \
--conf spark.sql.cache.serializer=com.nvidia.spark.rapids.shims.spark311.ParquetCachedBatchSerializer \
--conf spark.rapids.sql.csv.read.float.enabled=true \
--conf spark.rapids.sql.csv.read.integer.enabled=true \
--conf spark.rapids.sql.variableFloatAgg.enabled=true \
run_benchmark.py data/100M_R100000000_P100_csv \
-x 4g -d 4g \
-o output/test_gpu_100M_clearcache \
-t column \
-l "sum,avg,(x+y),avg(x+y),sum(x+y)" \
-n 5 \
--clearcache

/opt/hadoop/bin/hadoop dfs -rm -r -f temp