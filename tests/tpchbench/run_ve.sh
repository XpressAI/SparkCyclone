#!/usr/bin/env bash

export SPARK_HOME=/opt/spark

time $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --num-executors=8 --executor-cores=1 --executor-memory=8G \
    --deploy-mode client \
    --name TPC-H_VE_$1 \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/cyclone/spark-cyclone-sql-plugin.jar \
    --conf spark.executor.extraClassPath=/opt/cyclone/spark-cyclone-sql-plugin.jar \
    --conf spark.plugins=com.nec.spark.AuroraSqlPlugin \
    --conf spark.sql.columnVector.offheap.enabled=true \
    --conf spark.com.nec.native-csv=false \
    --conf spark.executor.resource.ve.amount=1 \
    --conf spark.executor.resource.ve.discoveryScript=/opt/spark/getVEsResources.sh \
    --conf spark.com.nec.spark.kernel.directory=/opt/spark/work/cyclone \
    --conf spark.executorEnv.VE_OMP_NUM_THREADS=1 \
    --conf spark.sql.codegen.wholeStage=false \
    --conf spark.com.nec.spark.enable-cache=true \
    --conf spark.com.nec.spark.sort-on-ve=true \
    --conf spark.com.nec.spark.project-on-ve=true \
    --conf spark.com.nec.spark.filter-on-ve=true \
    --conf spark.sql.cache.serializer=com.nec.spark.planning.VeCachedBatchSerializer \
    target/scala-2.12/tpchbench_2.12-0.0.1.jar \
    $1 $2

true
