#!/bin/bash

/opt/spark/bin/spark-submit --master yarn \
--name CPU_Benchmark \
--deploy-mode cluster \
--py-files dep.zip \
run_benchmark.py data/100M_R100000000_P100_csv \
-x 4g -d 4g \
-o output/test_cpu_100M_clearcache \
-t column \
-l "sum,avg,(x+y),avg(x+y),sum(x+y)" \
-n 5 \
--clearcache

/opt/hadoop/bin/hadoop dfs -rm -r -f temp