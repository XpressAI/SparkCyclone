#!/usr/bin/env bash

$SPARK_HOME/bin/spark-submit \
    --master yarn \
    --num-executors=8 --executor-cores=1 --executor-memory=7G \
    --deploy-mode cluster \
    --name groups.py_VE \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /home/egonzalez/nec_spark/aurora4spark-remote/target/scala-2.12/aurora4spark-sql-plugin-assembly-0.1.0-SNAPSHOT.jar \
    --conf spark.executor.extraClassPath=/home/egonzalez/nec_spark/aurora4spark-remote/target/scala-2.12/aurora4spark-sql-plugin-assembly-0.1.0-SNAPSHOT.jar \
    --conf spark.plugins=com.nec.spark.AuroraSqlPlugin \
    --conf spark.sql.columnVector.offheap.enabled=true \
    --conf spark.com.nec.native-csv=false \
    --conf spark.executor.resource.ve.amount=1 \
    --conf spark.executor.resource.ve.discoveryScript=/opt/spark/getVEsResources.sh \
    --conf spark.com.nec.spark.kernel.directory=/opt/spark/work/egonzalez \
    --conf spark.executorEnv.VE_OMP_NUM_THREADS=1 \
    --conf spark.executorEnv.VE_PROGINF=YES \
    groups.py

#   --conf spark.executor.extraJavaOptions=-agentpath:/opt/yjp/bin/linux-x86-64/libyjpagent.so \

