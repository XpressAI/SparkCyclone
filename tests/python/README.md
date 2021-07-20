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
usage: run_benchmark.py [-h] [-n NTEST] [-cc] [-o OUTPUTFILE]
                        {column,group_by,nycdata} ...

Run Benchmark. Please generate dataset using generate_data.py first.

positional arguments:
  {column,group_by,nycdata}
                        Dataset Options
    column              Column Benchmark
    group_by            group-by Benchmark
    nycdata             NYC-taxi-data Benchmark

optional arguments:
  -h, --help            show this help message and exit
  -n NTEST, --ntest NTEST
                        Number of Tests
  -cc, --clearcache     Clear cache for every tasks
  -o OUTPUTFILE, --outputfile OUTPUTFILE
                        Output file name (CSV)
```

### run_bencmark.py column -h
```
usage: run_benchmark.py column [-h] [-l LIST] file_url

positional arguments:
  file_url              Input file URL

optional arguments:
  -h, --help            show this help message and exit
  -l LIST, --list LIST  Comma delimited list input
```
### run_benchmark.py nycdata -h
```
usage: run_benchmark.py nycdata [-h] [-l LIST]

optional arguments:
  -h, --help            show this help message and exit
  -l LIST, --list LIST  Comma delimited list input
```

### run_benchmark.py group_by -h
```
usage: run_benchmark.py group_by [-h] [-l LIST] file_url

positional arguments:
  file_url              Input file URL

optional arguments:
  -h, --help            show this help message and exit
  -l LIST, --list LIST  Comma delimited list input
```


### Queries Notes
- Need to list out operations otherwise it will compute all operations by default (-l / --list)
- List of operations stated in `queries.py`, take the functions name as query name

## Run with GPU

Download `rapids-4-spark_2.12-0.5.0.jar` and `cudf-0.19.2-cuda10-1.jar` from [here](https://nvidia.github.io/spark-rapids/docs/download.html), and run sample script `run_with_gpu.sh`

```
curl -O https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.5.0/rapids-4-spark_2.12-0.5.0.jar
curl -O https://repo1.maven.org/maven2/ai/rapids/cudf/0.19.2/cudf-0.19.2-cuda10-1.jar
```