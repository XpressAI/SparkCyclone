#!/bin/bash

zip dep.zip *.py

# VE
export VE_OMP_NUM_THREADS=1
export PYSPARK_PYTHON=python3

/opt/spark-2.3.2/bin/spark-submit --master yarn \
--deploy-mode cluster \
--name VE_Benchmark_column_100M \
--py-files dep.zip \
--num-executors=8 --executor-cores=1 --executor-memory=8G \
]--conf spark.sql.inMemoryColumnarStorage.batchSize=174592 \
--conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
--jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
--conf spark.driver.extraJavaOptions="-javaagent:/opt/aurora4spark/agent.jar" \
--conf spark.executor.extraJavaOptions="-javaagent:/opt/aurora4spark/agent.jar" \
--conf spark.plugins=com.nec.spark.AuroraSqlPlugin \
--conf spark.sql.columnVector.offheap.enabled=true \
--conf spark.com.nec.native-csv=false \
--conf spark.executorEnv.VE_OMP_NUM_THREADS=1 \
--conf spark.driver.extraClassPath=/opt/aurora4spark/aurora4spark-sql-plugin.jar \
--conf spark.executor.extraClassPath=/opt/aurora4spark/aurora4spark-sql-plugin.jar \
--conf spark.com.nec.spark.kernel.directory=/opt/spark-2.3.2/work/egonzalez \
run_benchmark.py  --outputfile "yarn_test_ve_100M" --ntest 5 \
column "data/XY_doubles_R100000000_P100_csv" \
--list "avg_x_double,avg_x_plus_y_double,sum_x_double,sum_x_plus_y_double,x_plus_y_double"

/opt/hadoop/bin/hadoop dfs -rm -r -f temp/

# GPU 
#/opt/spark-2.3.2/bin/spark-submit --master yarn \
#--name GPU_Benchmark_column_100M \
# --deploy-mode cluster \
# --py-files dep.zip \
# --num-executors=1 --executor-cores=1 --executor-memory=16G \
# --jars 'rapids.jar,cudf.jar' \
# --conf spark.plugins=com.nvidia.spark.SQLPlugin \
# --conf spark.rapids.sql.incompatibleOps.enabled=true \
# --conf spark.rapids.sql.explain=ALL \
# --conf spark.rapids.sql.csv.read.float.enabled=true \
# --conf spark.rapids.sql.csv.read.integer.enabled=true \
# --conf spark.rapids.sql.variableFloatAgg.enabled=true \
# --conf spark.sql.cache.serializer=com.nvidia.spark.rapids.shims.spark311.ParquetCachedBatchSerializer \
# --conf spark.rapids.sql.exec.CollectLimitExec=true \
# --conf spark.rapids.sql.csv.read.double.enabled=true \
# --conf spark.rapids.sql.csv.read.long.enabled=true \
# --conf spark.rapids.sql.castFloatToString.enabled=true \
# --conf spark.sql.columnVector.offheap.enabled=true \
# run_benchmark.py  --outputfile "yarn_test_gpu_100M" --clearcache --ntest 5 \
# column "data/XY_doubles_R100000000_P100_csv" \
# --list "avg_x_double,avg_x_plus_y_double,sum_x_double,sum_x_plus_y_double,x_plus_y_double"

/opt/hadoop/bin/hadoop dfs -rm -r -f temp/

# JVM
/opt/spark-2.3.2/bin/spark-submit --master yarn \
--deploy-mode cluster \
--name CPU_Benchmark_column_100M \
--py-files dep.zip \
--num-executors=2 --executor-cores=8 --executor-memory=8G \
--conf spark.sql.columnVector.offheap.enabled=true \
run_benchmark.py  --outputfile "yarn_test_cpu_100M" --ntest 5 \
column "data/XY_doubles_R100000000_P100_csv" \
--list "avg_x_double,avg_x_plus_y_double,sum_x_double,sum_x_plus_y_double,x_plus_y_double"

/opt/hadoop/bin/hadoop dfs -rm -r -f temp/
