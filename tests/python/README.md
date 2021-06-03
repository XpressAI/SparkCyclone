# PySpark Benchmark

## Generate Test Data
```py
spark-submit --master spark://spark-master:7077  generate_data.py /path/to/test/data/file -r num_rows -p num_partitions
```
### File Type supported (-ft / --filetype)
- CSV (default)
- parquet
- json

## Run Benchmark
```py
spark-submit --master spark://spark-master:7077 run_benchmark.py /path/to/test/data/file -r num_partitions  -o 'output' -sl 11001 -t column -l max,min
```
### Benchmark type (-t / --type)
- groubyagg
- repart
- innerjoin
- broadinnerjoin
- column

### Repartition Notes
- Need to state partition number (-r / --repartitions)
- 200 partitions by default

### Column Notes
- Need to list out operations otherwise it will compute all operations by default (-l / --list)
- List of operations stated in `column_operation_dict.py`
### Pick Storage Level (-sl / --storageLevel)
- DISK_ONLY = 10001
- DISK_ONLY_2 = 10002
- MEMORY_AND_DISK = 11001
- MEMORY_AND_DISK_2 = 11002
- MEMORY_AND_DISK_SER = 11001
- MEMORY_AND_DISK_SER_2 = 11002
- MEMORY_ONLY = 01001
- MEMORY_ONLY_2 = 01002
- MEMORY_ONLY_SER = 01001
- MEMORY_ONLY_SER_2 = 01002
- OFF_HEAP = 11101

## Run with GPU

Download `rapids-4-spark_2.12-0.5.0.jar` and `cudf-0.19.2-cuda10-1.jar` from [here](https://nvidia.github.io/spark-rapids/docs/download.html), and run sample script `run_with_gpu.sh`

```
curl -O https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.5.0/rapids-4-spark_2.12-0.5.0.jar
curl -O https://repo1.maven.org/maven2/ai/rapids/cudf/0.19.2/cudf-0.19.2-cuda10-1.jar
```