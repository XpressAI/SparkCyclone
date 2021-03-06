#!/usr/bin/env bash

export SPARK_HOME=/opt/spark

time $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --num-executors=8 --executor-cores=2 --executor-memory=8G \
    --deploy-mode cluster \
    --name TPC-H_VE_$1 \
    --jars /opt/cyclone/${USER}/spark-cyclone-sql-plugin.jar \
    --conf spark.executor.extraClassPath=/opt/cyclone/${USER}/spark-cyclone-sql-plugin.jar \
    --conf spark.plugins=io.sparkcyclone.plugin.AuroraSqlPlugin \
    --conf spark.sql.columnVector.offheap.enabled=true \
    --conf spark.executor.resource.ve.amount=1 \
    --conf spark.executor.resource.ve.discoveryScript=/opt/spark/getVEsResources.py \
    --conf spark.cyclone.kernel.directory=/opt/spark/work/cyclone \
    --conf spark.executorEnv.VE_OMP_NUM_THREADS=1 \
    --conf spark.cyclone.sql.aggregate-on-ve=true \
    --conf spark.cyclone.sql.sort-on-ve=true \
    --conf spark.cyclone.sql.project-on-ve=false \
    --conf spark.cyclone.sql.filter-on-ve=true \
    --conf spark.cyclone.sql.exchange-on-ve=true \
    --conf spark.cyclone.sql.join-on-ve=false \
    --conf spark.cyclone.sql.pass-through-project=false \
    --conf spark.cyclone.sql.fail-fast=false \
    --conf spark.cyclone.sql.amplify-batches=true \
    --conf spark.cyclone.ve.columnBatchSize=128000 \
    --conf spark.cyclone.ve.targetBatchSizeMb=32 \
    --conf spark.sql.inMemoryColumnarStorage.batchSize=128000 \
    --conf spark.sql.autoBroadcastJoinThreshold=-1 \
    --conf spark.shuffle.partitions=8 \
    target/scala-2.12/tpchbench-assembly-0.0.1.jar \
    $*
