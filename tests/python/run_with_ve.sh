#!/bin/bash

/opt/spark/bin/spark-submit \
--jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
--conf spark.plugins=com.nec.spark.AuroraSqlPlugin \
--conf spark.sql.extensions=com.nec.spark.LocalVeoExtension \
--conf spark.sql.columnVector.offheap.enabled=true \
run_benchmark.py data/100M_R100000000_P100_csv \
-x 4g -d 4g \
-o output/test_ve_100M_rows \
-t column \
-l sum,avg,sum_two_cols \
-n 5

/opt/hadoop/bin/hadoop dfs -rm -r -f temp