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
Example
```py
spark-submit --master spark://spark-master:7077 run_benchmark.py  --outputfile "test_cpu_nyc_taxi" --clearcache --ntest 5 nycdata --list "q5"
```
### run_benchmark.py -h
```
usage: run_benchmark.py [-h] [-x EXECUTOR] [-d DRIVER] [-sl STORAGELEVEL]
                        [-n NTEST] [-cc] [-o OUTPUTFILE]
                        {random,nycdata} ...

Run Benchmark. Please generate dataset using generate_data.py first.

positional arguments:
  {random,nycdata}      Dataset Options
    random              Random Dataset
    nycdata             NYC-taxi-data Dataset

optional arguments:
  -h, --help            show this help message and exit
  -x EXECUTOR, --executor EXECUTOR
                        Set Executor Memory
  -d DRIVER, --driver DRIVER
                        Set Driver Memory
  -sl STORAGELEVEL, --storageLevel STORAGELEVEL
                        Set Storage Level
  -n NTEST, --ntest NTEST
                        Number of Tests
  -cc, --clearcache     Clear cache for every tasks
  -o OUTPUTFILE, --outputfile OUTPUTFILE
                        Output file name (CSV)
```

### run_bencmark.py random -h
```
usage: run_benchmark.py random [-h] [-r REPARTITIONS]
                               [-t {groupbyagg,repart,innerjoin,broadinnerjoin,column}]
                               [-l LIST]
                               file_url

positional arguments:
  file_url              Input file URL

optional arguments:
  -h, --help            show this help message and exit
  -r REPARTITIONS, --repartitions REPARTITIONS
                        Number of partitions
  -t {groupbyagg,repart,innerjoin,broadinnerjoin,column}, --type {groupbyagg,repart,innerjoin,broadinnerjoin,column}
                        Set Benchmark Type
  -l LIST, --list LIST  Comma delimited list input
```
### run_benchmark.py nycdata -h
```
usage: run_benchmark.py nycdata [-h] [-l LIST]

optional arguments:
  -h, --help            show this help message and exit
  -l LIST, --list LIST  Comma delimited list input
```

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