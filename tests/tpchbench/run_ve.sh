#!/usr/bin/env bash

export SPARK_HOME=/opt/spark

time $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --num-executors=8 --executor-cores=1 --executor-memory=8G \
    --deploy-mode cluster \
    --name TPC-H_VE \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    --conf spark.executor.extraClassPath=/opt/aurora4spark/aurora4spark-sql-plugin.jar \
    --conf spark.plugins=com.nec.spark.AuroraSqlPlugin \
    --conf spark.sql.columnVector.offheap.enabled=true \
    --conf spark.com.nec.native-csv=false \
    --conf spark.executor.resource.ve.amount=1 \
    --conf spark.executor.resource.ve.discoveryScript=/opt/spark/getVEsResources.sh \
    --conf spark.com.nec.spark.kernel.precompiled=/opt/spark/work/egonzalez \
    --conf spark.executorEnv.VE_OMP_NUM_THREADS=1 \
    --conf com.nec.spark.preshuffle-partitions=8 \
    target/scala-2.12/tpchbench_2.12-0.0.1.jar

#    --conf spark.sql.codegen.wholeStage=false \

