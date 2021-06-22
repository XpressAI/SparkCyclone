#!/bin/bash

zip dep.zip *.py

/opt/spark/bin/spark-submit --master yarn \
--name VE_Benchmark \
--deploy-mode cluster \
--py-files dep.zip \
--conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
--jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
--conf spark.plugins=com.nec.spark.AuroraSqlPlugin \
--conf spark.sql.columnVector.offheap.enabled=true \
run_benchmark.py  --outputfile "test_ve_nyc_taxi" --clearcache --ntest 5 nycdata --list "q1,q2,q3,q4,q5"


/opt/hadoop/bin/hadoop dfs -rm -r -f temp
