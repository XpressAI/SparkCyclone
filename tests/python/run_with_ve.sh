#!/bin/bash

# zip dep.zip *.py

#--deploy-mode cluster \

# /opt/spark/bin/spark-submit --master local[1] \
# --name VE_Benchmark \
# --py-files dep.zip \
# --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
# --jars /opt/cyclone/spark-cyclone-sql-plugin.jar \
# --conf spark.plugins=com.nec.spark.AuroraSqlPlugin \
# --conf spark.sql.columnVector.offheap.enabled=true \
# run_benchmark.py  --outputfile "test_ve_nyc_taxi" --clearcache --ntest 5 nycdata --list "q1,q2,q3,q4,q5"

# export VE_OMP_NUM_THREADS=1

# /opt/spark/bin/spark-submit --master yarn \
#     --deploy-mode cluster \
#     --name VE_Benchmark \
#     --py-files dep.zip \
#     --num-executors=1 --executor-cores=1 --executor-memory=40G \
#     --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
#     --jars /opt/cyclone/spark-cyclone-sql-plugin.jar \
#     --conf spark.plugins=com.nec.spark.AuroraSqlPlugin \
#     --conf spark.sql.columnVector.offheap.enabled=true \
#     --conf spark.com.nec.native-csv=VE \
#     --conf spark.executor.resource.ve.amount=1 \
#     --conf spark.executor.resource.ve.discoveryScript=/opt/spark/getVEsResources.sh \
#     --conf spark.executorEnv.VE_OMP_NUM_THREADS=8 \
#     --conf spark.executorEnv.VE_PROGINF=YES \
#     run_benchmark.py \
#     --outputfile output/test_ve_1m \
#     --clearcache \
#     --ntest 5 \
#     column \
#     "data/XY_doubles_R1000000_P100_csv" \
#     --list "avg_x_double,avg_x_plus_y_double,sum_x_double,sum_x_plus_y_double,x_plus_y_double"

# /opt/hadoop/bin/hdfs dfs -rm -r -f temp

zip dep.zip *.py

# VE
export VE_OMP_NUM_THREADS=1
/opt/spark/bin/spark-submit --master yarn \
--deploy-mode cluster \
--name VE_Benchmark_column_1K \
--py-files dep.zip \
--num-executors=8 --executor-cores=1 --executor-memory=16G \
--conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
--jars /opt/cyclone/spark-cyclone-sql-plugin.jar \
--conf spark.plugins=com.nec.spark.AuroraSqlPlugin \
--conf spark.sql.columnVector.offheap.enabled=true \
--conf spark.com.nec.native-csv=VE \
--conf spark.executorEnv.VE_OMP_NUM_THREADS=1 \
--conf spark.executor.extraClassPath=/opt/cyclone/spark-cyclone-sql-plugin.jar \
--conf spark.driver.resource.ve.amount=1 \
--conf spark.executor.resource.ve.amount=1 \
--conf spark.resources.discoveryPlugin=com.nec.ve.DiscoverVectorEnginesPlugin \
--conf spark.com.nec.spark.kernel.directory=/opt/spark/work/muhdlaziem \
run_benchmark.py  --outputfile "yarn_test_ve_1K" --clearcache --ntest 5 \
column "data/XY_doubles_R1000_P100_csv" \
--list "avg_x_double,avg_x_plus_y_double,sum_x_double,sum_x_plus_y_double,x_plus_y_double"

/opt/hadoop/bin/hadoop dfs -rm -r -f temp/
