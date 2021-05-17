#!/bin/bash

/opt/spark/bin/spark-submit \
--jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
run_benchmark.py data/10M_R10000000_P100_csv/ \
-x 2g -d 1g \
-o output/sum_avg_gpu \
-t column \
-l sum,avg \
-n 10
