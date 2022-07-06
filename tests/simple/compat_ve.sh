#!/usr/bin/env bash

$SPARK_HOME/bin/spark-submit \
    --master yarn \
    --num-executors=8 --executor-cores=1 --executor-memory=7G \
    --name compat.py_VE \
    --conf spark.cyclone.ncc.path=/opt/nec/ve/bin/ncc \
    --conf spark.sql.inMemoryColumnarStorage.batchSize=174592 \
    --conf spark.sql.files.minPartitionNum=8 \
    --jars /opt/cyclone/spark-cyclone-sql-plugin.jar \
    --conf spark.executor.extraClassPath=/opt/cyclone/spark-cyclone-sql-plugin.jar \
    --conf spark.plugins=io.sparkcyclone.spark.AuroraSqlPlugin \
    --conf spark.sql.columnVector.offheap.enabled=true \
    --conf spark.driver.resource.ve.amount=1 \
    --conf spark.driver.resource.ve.discoveryScript=/opt/spark/getVEsResources.sh \
    --conf spark.executor.resource.ve.amount=1 \
    --conf spark.executor.resource.ve.discoveryScript=/opt/spark/getVEsResources.sh \
    --conf spark.cyclone.kernel.directory=/opt/spark/work/egonzalez \
    --conf spark.executorEnv.VE_OMP_NUM_THREADS=1 \
    --conf spark.executorEnv.VE_PROGINF=YES \
    compat.py

#   --conf spark.executor.extraJavaOptions=-agentpath:/opt/yjp/bin/linux-x86-64/libyjpagent.so \

