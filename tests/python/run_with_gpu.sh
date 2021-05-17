#!/bin/bash

/opt/spark/bin/spark-submit --master spark://master:7077 \
--jars 'rapids-4-spark_2.12-0.4.1.jar,cudf-0.18.1-cuda10-1.jar' \
--conf spark.plugins=com.nvidia.spark.SQLPlugin \
--conf spark.rapids.sql.incompatibleOps.enabled=true \
run_benchmark.py data/10M_R10000000_P100_csv/ \
-x 2g -d 1g \
-o output/sum_avg_gpu \
-t column \
-l sum,avg \
-n 10