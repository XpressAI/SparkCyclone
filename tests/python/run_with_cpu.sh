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

# /opt/hadoop/bin/hadoop dfs -rm -r -f temp/

zip dep.zip *.py

/opt/spark/bin/spark-submit --master yarn \
    --deploy-mode cluster \
    --name CPU_Benchmark_column_100M_2_exec_12_cores \
    --py-files dep.zip \
    --num-executors=2 --executor-cores=12 --executor-memory=16G \
    --conf spark.sql.columnVector.offheap.enabled=true \
    run_benchmark.py  --outputfile "yarn_test_cpu_100M_2_exec_12_cores" --clearcache --ntest 5 \
    column "data/XY_doubles_R100000000_P100_csv" \
    --list "avg_x_double,avg_x_plus_y_double,sum_x_double,sum_x_plus_y_double,x_plus_y_double"

/opt/hadoop/bin/hdfs dfs -rm -r -f temp/
