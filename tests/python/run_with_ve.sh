#!/bin/bash

zip dep.zip *.py

#--deploy-mode cluster \

#/opt/spark/bin/spark-submit --master local[1] \
#--name VE_Benchmark \
#--py-files dep.zip \
#--conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
#--jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
#--conf spark.plugins=com.nec.spark.AuroraSqlPlugin \
#--conf spark.sql.columnVector.offheap.enabled=true \
#run_benchmark.py  --outputfile "test_ve_nyc_taxi" --clearcache --ntest 5 nycdata --list "q1,q2,q3,q4,q5"

export VE_OMP_NUM_THREADS=1

/opt/spark/bin/spark-submit --master yarn \
    --deploy-mode cluster \
    --name VE_Benchmark \
    --py-files dep.zip \
    --num-executors=1 --executor-cores=1 --executor-memory=40G \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    --conf spark.plugins=com.nec.spark.AuroraSqlPlugin \
    --conf spark.sql.columnVector.offheap.enabled=true \
    --conf spark.com.nec.native-csv=VE \
    --conf spark.executor.resource.ve.amount=1 \
    --conf spark.executor.resource.ve.discoveryScript=/opt/spark/getVEsResources.sh \
    --conf spark.executorEnv.VE_OMP_NUM_THREADS=8 \
    --conf spark.executorEnv.VE_PROGINF=YES \
    run_benchmark.py \
    --outputfile output/test_ve_1m \
    --clearcache \
    --ntest 5 \
    random \
    -l "sum_float,avg_float,(x+y)_float,avg(x+y)_float,sum(x+y)_float" \
    -t column \
    hdfs:///data/test_file_1m_3f_R1000000_P1000_csv

/opt/hadoop/bin/hdfs dfs -rm -r -f temp
