#!/usr/bin/env bash

export SPARK_HOME=/opt/spark

time $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --num-executors=8 --executor-cores=2 --executor-memory=8G \
    --deploy-mode cluster \
    --name TPC-H_CPU_$1 \
    --conf spark.sql.codegen.wholeStage=false \
    target/scala-2.12/tpchbench-assembly-0.0.1.jar \
    $*

#    --conf spark.sql.adaptive.enabled=true \
 #    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
 # --conf spark.sql.codegen.wholeStage=false \
   #    --conf spark.sql.inMemoryColumnarStorage.batchSize=128000 \
   #    --conf spark.sql.autoBroadcastJoinThreshold=-1 \
   #    --conf spark.shuffle.partitions=8 \
   #
