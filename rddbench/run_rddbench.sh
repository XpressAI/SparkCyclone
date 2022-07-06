#!/usr/bin/env bash

export SPARK_HOME=/opt/spark

time $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --num-executors=8 --executor-cores=1 --executor-memory=8G --driver-memory=8G \
    --deploy-mode cluster \
    --name RDDBench \
    --conf spark.cyclone.ncc.path=/opt/nec/ve/bin/ncc \
    --jars ../target/scala-2.12/spark-cyclone-sql-plugin-assembly-1.0.5-SNAPSHOT.jar \
    --conf spark.executor.extraClassPath=../target/scala-2.12/spark-cyclone-sql-plugin-assembly-1.0.5-SNAPSHOT.jar \
    --conf spark.rpc.message.maxSize=1024 \
    --conf spark.plugins=io.sparkcyclone.spark.AuroraSqlPlugin \
    --conf spark.sql.columnVector.offheap.enabled=true \
    --conf spark.executor.resource.ve.amount=1 \
    --conf spark.executor.resource.ve.discoveryScript=/opt/spark/getVEsResources.sh \
    --conf spark.cyclone.kernel.directory=/opt/spark/work/cyclone \
    --conf spark.executorEnv.VE_OMP_NUM_THREADS=1 \
    --conf spark.yarn.maxAppAttempts=1 \
    --conf spark.task.maxFailures=1 \
    target/scala-2.12/rddbench_2.12-0.1.jar \
    $*

#--conf spark.locality.wait=0 \
#        --conf spark.driverEnv.VEO_LOG_DEBUG=1 \

 #   --conf spark.executorEnv.VEO_LOG_DEBUG=1 \
#    --conf spark.executorEnv.VEO_LOG_DEBUG=1 \
#    --conf spark.driver.extraJavaOptions="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005" \