# PySpark Benchmark

## Generate Test Data
```py
spark-submit --master spark://spark-master:7077  generate_data.py /path/to/test/data/file -r num_rows -p num_partitions
```

## Run Benchmark
```py
spark-submit --master spark://spark-master:7077 run_benchmark.py /path/to/test/data/file -r num_partitions  -o 'output' -sl 11001 -t column -l max,min
```

## Pick Level
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