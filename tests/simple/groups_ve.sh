#!/usr/bin/env bash

$SPARK_HOME/bin/spark-submit \
    --master yarn \
    --num-executors=8 --executor-cores=1 --executor-memory=7G \
    --deploy-mode cluster \
    --name groups.py_VE \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    --conf spark.executor.extraClassPath=/opt/aurora4spark/aurora4spark-sql-plugin.jar \
    --conf spark.plugins=com.nec.spark.AuroraSqlPlugin \
    --conf spark.sql.columnVector.offheap.enabled=true \
    --conf spark.com.nec.native-csv=false \
    --conf spark.driver.resource.ve.amount=1 \
    --conf spark.driver.resource.ve.discoveryScript=/opt/spark/getVEsResources.sh \
    --conf spark.executor.resource.ve.amount=1 \
    --conf spark.executor.resource.ve.discoveryScript=/opt/spark/getVEsResources.sh \
    --conf spark.com.nec.spark.kernel.precompiled=/opt/spark/work/egonzalez \
    --conf spark.executorEnv.VE_OMP_NUM_THREADS=1 \
    groups.py

#    --conf spark.executorEnv.VE_PROGINF=YES \
#        --conf spark.executor.extraJavaOptions=-agentpath:/opt/yjp/bin/linux-x86-64/libyjpagent.so \

