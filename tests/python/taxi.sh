#!/usr/bin/env bash

zip dep.zip *.py

/opt/hadoop/bin/hdfs dfs -rm -r -f temp/

/opt/spark/bin/spark-submit --master yarn \
    --deploy-mode cluster \
    --name VE_Benchmark_nyc_data \
    --py-files dep.zip \
    --num-executors=8 --executor-cores=1 --executor-memory=7G \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/cyclone/spark-cyclone-sql-plugin.jar \
    --conf spark.plugins=io.sparkcyclone.spark.AuroraSqlPlugin \
    --conf spark.sql.columnVector.offheap.enabled=true \
    --conf spark.com.nec.native-csv=false \
    --conf spark.executorEnv.VE_OMP_NUM_THREADS=1 \
    --conf spark.executor.extraClassPath=/opt/cyclone/spark-cyclone-sql-plugin.jar \
    --conf spark.driver.resource.ve.amount=1 \
    --conf spark.executor.resource.ve.amount=1 \
    --conf spark.resources.discoveryPlugin=com.nec.ve.DiscoverVectorEnginesPlugin \
    --conf spark.com.nec.spark.kernel.directory=/opt/spark/work/egonzalez \
    run_benchmark.py  --outputfile "yarn_test_taxi_ve" --ntest 5 \
    nycdata

/opt/spark/bin/spark-submit --master yarn \
    --deploy-mode cluster \
    --name CPU_Benchmark_nyc_data \
    --py-files dep.zip \
    --num-executors=2 --executor-cores=8 --executor-memory=8G \
    --conf spark.sql.columnVector.offheap.enabled=true \
    run_benchmark.py  --outputfile "yarn_test_taxi_cpu" --ntest 5 \
    nycdata

