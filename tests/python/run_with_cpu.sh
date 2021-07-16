#!/bin/bash

zip dep.zip *.py

/opt/spark/bin/spark-submit --master yarn \
    --num-executors=16 --executor-cores=1 --executor-memory=3G \
    --deploy-mode cluster \
    --name VE_Benchmark \
    --py-files dep.zip \
    --conf spark.sql.columnVector.offheap.enabled=true \
    run_benchmark.py \
    --outputfile output/test_cpu_10b \
    --clearcache \
    --ntest 3 \
    random \
    -l "sum_float,avg_float,(x+y)_float,avg(x+y)_float,sum(x+y)_float" \
    -t column \
    hdfs:///data/test_file_10b_3f_R10000000000_P10000_csv

#/opt/hadoop/bin/hadoop dfs -rm -r -f temp
