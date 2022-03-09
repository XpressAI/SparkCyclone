#!/usr/bin/env bash

export SPARK_HOME=/opt/spark

time $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --num-executors=8 --executor-cores=1 --executor-memory=8G \
    --deploy-mode cluster \
    --name TPC-H_VE_$1 \
    --jars /opt/cyclone/${USER}/spark-cyclone-sql-plugin.jar \
    --conf spark.executor.extraClassPath=/opt/cyclone/${USER}/spark-cyclone-sql-plugin.jar \
    --conf spark.plugins=com.nec.spark.AuroraSqlPlugin \
    --conf spark.sql.columnVector.offheap.enabled=true \
    --conf spark.executor.resource.ve.amount=1 \
    --conf spark.executor.resource.ve.discoveryScript=/opt/spark/getVEsResources.py \
    --conf spark.com.nec.spark.kernel.directory=/opt/spark/work/cyclone \
    --conf spark.executorEnv.VE_OMP_NUM_THREADS=1 \
    --conf spark.com.nec.spark.aggregate-on-ve=true \
    --conf spark.com.nec.spark.sort-on-ve=false \
    --conf spark.com.nec.spark.project-on-ve=false \
    --conf spark.com.nec.spark.filter-on-ve=true \
    --conf spark.com.nec.spark.exchange-on-ve=true \
    --conf spark.com.nec.spark.join-on-ve=true \
    --conf spark.com.nec.spark.pass-through-project=false \
    --conf spark.com.nec.spark.fail-fast=false \
    --conf spark.com.nec.spark.amplify-batches=true \
    --conf spark.com.nec.spark.ve.columnBatchSize=128000 \
    --conf spark.com.nec.spark.ve.targetBatchSizeMb=32 \
    --conf spark.sql.inMemoryColumnarStorage.batchSize=128000 \
    --conf spark.sql.codegen.wholeStage=false \
    --conf spark.sql.autoBroadcastJoinThreshold=-1 \
    --conf spark.shuffle.partitions=8 \
    --conf spark.executorEnv.VE_PROGINF=YES \
    target/scala-2.12/tpchbench-assembly-0.0.1.jar \
    $*


#    --conf spark.executorEnv.VE_ACC_IO=1 \
#    --conf spark.executorEnv.VEO_LOG_DEBUG=1 \