#!/bin/bash

/opt/spark/bin/spark-submit --master yarn \
--name CPU_Benchmark \
--deploy-mode cluster \
--py-files dep.zip \
run_benchmark.py data/100M_R100000000_P100_csv \
-x 4g -d 4g \
-o output/test_cpu_100M_clearcache \
-t column \
-l "sum_int,sum_float,avg_int,avg_float,(x+y)_int,(x+y)_float,avg(x+y)_int,avg(x+y)_float,sum(x+y)_int,sum(x+y)_float" \
-n 5 \
--clearcache

/opt/hadoop/bin/hadoop dfs -rm -r -f temp