# Tuning Aurora4Spark

Tuning a spark job will improve performace in most cases. Having said that, finding the good configuration is always a difficult task.

## Number of Executors

We need to specify the `--num-executors` equals to the number of Vector Engine cores to maximize the job performance

## Avoiding OpenMP from overloading the Vector Engine

If `--num-executors` is set to number of Vector Engine cores, OpenMP might overload the Vector Engine. To disable any OpenMP from happening, we can specify `--conf spark.executorEnv.VE_OMP_NUM_THREADS=1`  which allows only 1 thread on single process

## Using Precompiled Directory

Using precompiled directory speeds up the performance. To use that, we need to specify first `--conf spark.com.nec.spark.kernel.directory=/path/to/precompiled` on the first run. After that we can specify `--conf spark.com.nec.spark.kernel.precompiled=/path/to/precompiled` to use precompiled directory.