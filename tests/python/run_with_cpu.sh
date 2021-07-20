#!/bin/bash

# zip dep.zip *.py

# /opt/spark/bin/spark-submit --master yarn \
#     --num-executors=24 --executor-cores=2 --executor-memory=16G \
#     --deploy-mode cluster \
#     --name CPU_Benchmark \
#     --py-files dep.zip \
#     --conf spark.sql.columnVector.offheap.enabled=true \
#     run_benchmark.py \
#     --outputfile test_cpu \
#     --ntest 3 \
#     column \
#     "data/XY_doubles_R1000000_P100_csv" \
#     --list "avg_x_double,avg_x_plus_y_double,sum_x_double,sum_x_plus_y_double,x_plus_y_double"

#/opt/hadoop/bin/hadoop dfs -rm -r -f temp

zip dep.zip *.py

/opt/spark/bin/spark-submit --master yarn \
    --num-executors=24 --executor-cores=2 --executor-memory=16G \
    --deploy-mode cluster \
    --name CPU_Benchmark \
    --py-files dep.zip \
    --conf spark.sql.columnVector.offheap.enabled=true \
    run_benchmark.py \
    --outputfile test_cpu \
    --ntest 3 \
    group_by \
    "data/XY_doubles_R1000000_P100_csv"