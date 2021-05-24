#!/bin/bash

/opt/spark/bin/spark-submit --master yarn \
run_benchmark.py data/100M_R100000000_P100_csv \
-x 4g -d 4g \
-o output/test_cpu_100M_rows \
-t column \
-l sum,avg,sum_two_cols \
-n 5

/opt/hadoop/bin/hadoop dfs -rm -r -f temp