#!/usr/bin/env bash

export SPARK_HOME=/opt/spark

time $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --num-executors=1 --executor-cores=1 --executor-memory=8G \
    --name TPC-H_GPU_$1 \
    --deploy-mode cluster \
    --jars 'rapids.jar,cudf.jar' \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.rapids.sql.incompatibleOps.enabled=true \
    --conf spark.rapids.sql.explain=NONE \
    --conf spark.rapids.sql.csv.read.float.enabled=true \
    --conf spark.rapids.sql.csv.read.integer.enabled=true \
    --conf spark.rapids.sql.variableFloatAgg.enabled=true \
    --conf spark.sql.cache.serializer=com.nvidia.spark.rapids.shims.spark311.ParquetCachedBatchSerializer \
    --conf spark.rapids.sql.exec.CollectLimitExec=true \
    --conf spark.rapids.sql.csv.read.double.enabled=true \
    --conf spark.rapids.sql.csv.read.long.enabled=true \
    --conf spark.rapids.sql.castFloatToString.enabled=true \
    --conf spark.sql.columnVector.offheap.enabled=true \
    --conf spark.rapids.sql.metrics.level=ESSENTIAL \
    target/scala-2.12/tpchbench-assembly-0.0.1.jar \
    $1 $2
