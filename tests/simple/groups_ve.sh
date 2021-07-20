#!/usr/bin/env bash

$SPARK_HOME/bin/spark-submit \
    --master yarn \
    --num-executors=8 --executor-cores=1 --executor-memory=4G \
    --deploy-mode cluster \
    --name groups.py_VE \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    --conf spark.plugins=com.nec.spark.AuroraSqlPlugin \
    --conf spark.sql.columnVector.offheap.enabled=true \
    --conf spark.com.nec.native-csv=VE \
    --conf spark.driver.resource.ve.amount=1 \
    --conf spark.driver.resource.ve.discoveryScript=/opt/spark/getVEsResources.sh \
    --conf spark.executor.resource.ve.amount=1 \
    --conf spark.executor.resource.ve.discoveryScript=/opt/spark/getVEsResources.sh \
    --conf spark.executorEnv.VE_OMP_NUM_THREADS=1 \
    --conf spark.executorEnv.VE_PROGINF=YES \
    groups.py
