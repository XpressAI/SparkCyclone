#!/bin/bash

zip dep.zip *.py

/opt/spark/bin/spark-submit --master "local[*]" \
--executor-memory 5g \
--driver-memory 5g \
--name VE_Benchmark_random \
--py-files dep.zip \
--conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
--jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
--conf spark.plugins=com.nec.spark.AuroraSqlPlugin \
--conf spark.sql.columnVector.offheap.enabled=true \
--conf spark.com.nec.native-csv=VE \
run_benchmark.py  --outputfile "test_ve_large" --clearcache --ntest 5 large "hdfs://localhost:9000/user/william/data/large-sample-csv-10_9" --list "q1"

/opt/hadoop/bin/hadoop dfs -rm -r -f temp
# --deploy-mode cluster \
