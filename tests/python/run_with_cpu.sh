#!/bin/bash

zip dep.zip *.py

/opt/spark/bin/spark-submit --master "local[*]" \
--executor-memory 5g \
--driver-memory 5g \
--name CPU_Benchmark \
--py-files dep.zip \
run_benchmark.py  --outputfile "test_cpu_large" --clearcache --ntest 5 large "hdfs://localhost:9000/user/william/data/large-sample-csv-10_9" --list "q1"

/opt/hadoop/bin/hadoop dfs -rm -r -f temp
# --deploy-mode cluster \
