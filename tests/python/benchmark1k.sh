#!/bin/bash

zip dep.zip *.py

# VE
export VE_OMP_NUM_THREADS=1
/opt/spark/bin/spark-submit --master yarn \
--deploy-mode cluster \
--name VE_Benchmark_column_1K \
--py-files dep.zip \
--num-executors=8 --executor-cores=1 --executor-memory=7G \
--conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
--conf spark.sql.inMemoryColumnarStorage.batchSize=174592 \
--jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
--conf spark.plugins=com.nec.spark.AuroraSqlPlugin \
--conf spark.sql.columnVector.offheap.enabled=true \
--conf spark.com.nec.native-csv=false \
--conf spark.executorEnv.VE_OMP_NUM_THREADS=1 \
--conf spark.executor.extraClassPath=/opt/aurora4spark/aurora4spark-sql-plugin.jar \
--conf spark.driver.resource.ve.amount=1 \
--conf spark.executor.resource.ve.amount=1 \
--conf spark.resources.discoveryPlugin=com.nec.ve.DiscoverVectorEnginesPlugin \
--conf spark.com.nec.spark.kernel.directory=/opt/spark/work/egonzalez \
run_benchmark.py  --outputfile "yarn_test_ve_1K" --ntest 10 \
group_by "data/XY_doubles_R1000_P100_csv" \
--list "group_by_avg_x,group_by_avg_x_plus_y"
#column "data/XY_doubles_R1000_P100_csv" \
#--list "avg_x_double,avg_x_plus_y_double,sum_x_double,sum_x_plus_y_double"

/opt/hadoop/bin/hadoop dfs -rm -r -f temp/


# JVM
/opt/spark/bin/spark-submit --master yarn \
--deploy-mode cluster \
--name CPU_Benchmark_column_1K \
--py-files dep.zip \
--num-executors=2 --executor-cores=8 --executor-memory=8G \
--conf spark.sql.columnVector.offheap.enabled=true \
run_benchmark.py  --outputfile "yarn_test_cpu_1K" --ntest 10 \
group_by "data/XY_doubles_R1000_P100_csv" \
--list "group_by_avg_x,group_by_avg_x_plus_y"
#column "data/XY_doubles_R1000_P100_csv" \
#--list "avg_x_double,avg_x_plus_y_double,sum_x_double,sum_x_plus_y_double"

/opt/hadoop/bin/hadoop dfs -rm -r -f temp/
