#!/bin/bash

zip dep.zip *.py

/opt/spark/bin/spark-submit --master yarn \
--name CPU_Benchmark \
--deploy-mode cluster \
--py-files dep.zip \
run_benchmark.py  --outputfile "test_cpu_nyc_taxi" --clearcache --ntest 5 nycdata --list "q1,q2,q3,q4,q5"


/opt/hadoop/bin/hadoop dfs -rm -r -f temp
