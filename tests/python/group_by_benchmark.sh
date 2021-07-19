#!/bin/bash

zip dep.zip *.py

# VE
export VE_OMP_NUM_THREADS=1
/opt/spark/bin/spark-submit --master yarn \
--deploy-mode cluster \
--name VE_Benchmark_group_by_1K \
--py-files dep.zip \
--num-executors=8 --executor-cores=6 --executor-memory=16G \
--conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
--jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
--conf spark.plugins=com.nec.spark.AuroraSqlPlugin \
--conf spark.sql.columnVector.offheap.enabled=true \
--conf spark.com.nec.native-csv=VE \
--conf spark.executorEnv.VE_OMP_NUM_THREADS=1 \
run_benchmark.py  --outputfile "yarn_test_ve_1K" --ntest 5 \
group_by "data/XY_doubles_R1000_P100_csv" \
--list "q1,q2"

# GPU 
/opt/spark/bin/spark-submit --master yarn \
--name GPU_Benchmark_group_by_1K \
--deploy-mode cluster \
--py-files dep.zip \
--jars 'rapids.jar,cudf.jar' \
--num-executors=1 --executor-cores=1 --executor-memory=16G \
--conf spark.plugins=com.nvidia.spark.SQLPlugin \
--conf spark.rapids.sql.incompatibleOps.enabled=true \
--conf spark.rapids.sql.explain=ALL \
--conf spark.rapids.sql.csv.read.float.enabled=true \
--conf spark.rapids.sql.csv.read.integer.enabled=true \
--conf spark.rapids.sql.variableFloatAgg.enabled=true \
--conf spark.sql.cache.serializer=com.nvidia.spark.rapids.shims.spark311.ParquetCachedBatchSerializer \
--conf spark.rapids.sql.exec.CollectLimitExec=true \
--conf spark.rapids.sql.csv.read.double.enabled=true \
--conf spark.rapids.sql.csv.read.long.enabled=true \
--conf spark.rapids.sql.castFloatToString.enabled=true \
run_benchmark.py  --outputfile "yarn_test_gpu_1K" --ntest 5 \
group_by "data/XY_doubles_R1000_P100_csv" \
--list "q1,q2"

# JVM
/opt/spark/bin/spark-submit --master yarn \
--deploy-mode cluster \
--name CPU_Benchmark_group_by_1K \
--py-files dep.zip \
--num-executors=24 --executor-cores=2 --executor-memory=16G \
run_benchmark.py  --outputfile "yarn_test_cpu_1K" --ntest 5 \
group_by "data/XY_doubles_R1000_P100_csv" \
--list "q1,q2"
