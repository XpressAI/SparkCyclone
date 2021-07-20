from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark import SparkConf
from pyspark import StorageLevel

import argparse
import sys
from timeit import default_timer as timer
import os

from queries import column_queries, group_by_queries, nyctaxi_queries

def arguments() -> argparse.Namespace:
    args = argparse.ArgumentParser(description='Run Benchmark. Please generate dataset using generate_data.py first.')
    # args.add_argument('-x','--executor', type=str, default='4g', help='Set Executor Memory')
    # args.add_argument('-d','--driver', type=str, default='4g', help='Set Driver Memory')
    # args.add_argument('-sl','--storageLevel', type=str, default='11001', help='Set Storage Level')
    args.add_argument('-n','--ntest', type=int, default=5, help='Number of Tests')
    args.add_argument('-cc', '--clearcache', action='store_true', default=False, help="Clear cache for every tasks")
    args.add_argument('-o','--outputfile', type=str, default=None, help='Output file name (CSV)')
    subparsers = args.add_subparsers(help='Dataset Options', dest="which")

    column = subparsers.add_parser('column', help='Column Benchmark')
    column.add_argument('inputfile', type=str, metavar='file_url', help='Input file URL')
    column.add_argument('-l', '--list', help='Comma delimited list input', type=lambda s: [item for item in s.split(',')], default=None)
    
    group_by = subparsers.add_parser('group_by', help='group-by Benchmark')
    group_by.add_argument('inputfile', type=str, metavar='file_url', help='Input file URL')
    group_by.add_argument('-l', '--list', help='Comma delimited list input', type=lambda s: [item for item in s.split(',')], default=None)
    
    nyc = subparsers.add_parser('nycdata', help='NYC-taxi-data Benchmark')
    nyc.add_argument('-l', '--list', help='Comma delimited list input', type=lambda s: [item for item in s.split(',')], default=None)

    return args.parse_args()
    
def benchmark(spark: SparkSession, args: argparse.Namespace, df: DataFrame = None) -> list:
    res = []
    dicts = None
    if(args.which == "column"): dicts = column_queries()
    elif(args.which == "group_by"): dicts = group_by_queries()
    elif(args.which == "nycdata"): dicts = nyctaxi_queries()
    else: raise Exception("Option not supported")
    
    ops = [method for method in dir(dicts) if method.startswith('__') is False] if args.list is None else args.list
    
    print("="*240)
    print(f'Starting Benchmark for {args.which}_benchmark() : {str(ops)}')

    for op in ops:
        col_op = [op]

        try:
            for i in range(args.ntest):
                spark.sparkContext.setLocalProperty('callSite.short', f'{op}_benchmark_test_{i}')
                print("="*240)
                print(f'Running {op}_benchmark_test_{i}')

                if (args.clearcache):
                    spark.catalog.clearCache() 

                start_time = timer()
                new_df = dicts[op](spark)
                new_df.write.csv(
                    f'temp/{op}_{i}',
                    header=True,
                    mode='overwrite'
                )
                time_taken = timer() - start_time
                new_df.explain(extended=True)
                print(f'Finished {op}_benchmark_test_{i} = {time_taken}')
                print("="*240)
                os.system("/opt/hadoop/bin/hdfs dfs -rm -r -f temp")
                col_op.append(time_taken)

            avg = (sum(col_op[1:]) - max(col_op[1:]) - min(col_op[1:])) / (args.ntest-2)
            print(f'AVG for {op}_benchmark_test = {avg}')

            col_op.append(avg)
            res.append(tuple(col_op))

        except Exception as e:
            print(f'Error {str(e)}. Skipping operation {op}')
    
    print("="*240)

    return res

# def parse_storage_level(level: list) -> list:
#     for i in range(4):
#         level[i] = bool(int(level[i]))
#     level[4] = int(level[4])
#     return level

def group_by_benchmark(spark: SparkSession, args: argparse.Namespace) -> list:
    print("="*240)
    print(f'Benchmark using input file = {args.inputfile}')
    print("="*240)

    schema = T.StructType([
        T.StructField("id", T.DoubleType()),
        T.StructField("double_x", T.DoubleType()),
        T.StructField("double_y", T.DoubleType()),
    ])
    
    df = spark.read.csv(args.inputfile, header=True, schema=schema)
    df.createOrReplaceTempView("table")
    
    if (args.clearcache is False):
        print('Caching Temp View Table...')
        spark.sql("CACHE TABLE table")
    df.printSchema()
    
    return benchmark(spark, args)

def column_benchmark(spark: SparkSession, args: argparse.Namespace) -> list:
    print("="*240)
    print(f'Benchmark using input file = {args.inputfile}')
    print("="*240)


    schema = T.StructType([
        # T.StructField("value", T.StringType()),
        # T.StructField("prefix2", T.StringType()),
        # T.StructField("prefix4", T.StringType()),
        # T.StructField("prefix8", T.StringType()),
        T.StructField("id", T.DoubleType()),
        T.StructField("double_x", T.DoubleType()),
        T.StructField("double_y", T.DoubleType()),
        #T.StructField("int_x", T.LongType()),
        #T.StructField("int_y", T.LongType()),
        # T.StructField("double_a", T.DoubleType()),
        #T.StructField("double_b", T.DoubleType()),
        #T.StructField("int_a", T.LongType()),
        #T.StructField("int_b", T.LongType()),
        # T.StructField("randn", T.LongType()),
        # T.StructField("randn1", T.DoubleType()),
        # T.StructField("degree", T.LongType()),
        # T.StructField("small_int", T.LongType()),
    ])
    # initialize df
    df = None
    
    if "csv" in args.inputfile:
        df = spark.read.csv(args.inputfile, header=True, schema=schema)
    elif "json" in args.inputfile:
        df = spark.read.json(args.inputfile, schema=schema)
    elif "parquet" in args.inputfile:
        df = spark.read.parquet(args.inputfile, schema=schema)

    assert df, (f"Filetype not found in {args.inputfile}! Ensure that the path dir is correct.")

    df.createOrReplaceTempView("table")
    
    if (args.clearcache is False):
        print('Caching Temp View Table...')
        spark.sql("CACHE TABLE table")
    df.printSchema()

    return benchmark(spark, args)

def nyc_benchmark(spark: SparkSession, args: argparse.Namespace) -> list:
    schema_nyctaxi = T.StructType([
        T.StructField("id",T.StringType(), False),
        T.StructField("cab_type_id",T.StringType()),
        T.StructField("vendor_id",T.StringType()),
        T.StructField("pickup_datetime",T.StringType()),
        T.StructField("dropoff_datetime",T.StringType()),
        T.StructField("store_and_fwd_flag",T.StringType()),
        T.StructField("rate_code_id",T.StringType()),
        T.StructField("pickup_longitude",T.StringType()),
        T.StructField("pickup_latitude",T.StringType()),
        T.StructField("dropoff_longitude",T.StringType()),
        T.StructField("dropoff_latitude",T.StringType()),
        T.StructField("passenger_count",T.LongType()),
        T.StructField("trip_distance",T.DoubleType()),
        T.StructField("fare_amount",T.DoubleType()),
        T.StructField("extra",T.DoubleType()),
        T.StructField("mta_tax",T.DoubleType()),
        T.StructField("tip_amount",T.DoubleType()),
        T.StructField("tolls_amount",T.DoubleType()),
        T.StructField("ehail_fee",T.DoubleType()),
        T.StructField("improvement_surcharge",T.DoubleType()),
        T.StructField("congestion_surcharge",T.DoubleType()),
        T.StructField("total_amount",T.DoubleType()),
        T.StructField("payment_type",T.DoubleType()),
        T.StructField("trip_type",T.StringType()),
        T.StructField("pickup_nyct2010_gid",T.StringType()),
        T.StructField("dropoff_nyct2010_gid",T.StringType()),
        T.StructField("pickup_location_id",T.StringType()),
        T.StructField("dropoff_location_id",T.StringType()),
    ])
    
    schema_cab = T.StructType([
        T.StructField("id", T.StringType(),False),
        T.StructField("type", T.StringType()),
    ])
    
    df = spark.read.csv('data/trips_2020.csv', header=True, schema=schema_nyctaxi)
    df1 = spark.read.csv('data/cab_types.csv', header=True, schema=schema_cab)
    df.createOrReplaceTempView('trips')
    df1.createOrReplaceTempView('cab_types')
    
    if (args.clearcache is False):
        print('Caching Temp View Table...')
        spark.sql("CACHE TABLE trips")
        spark.sql("CACHE TABLE cab_types")
    df.printSchema()
    df1.printSchema()

    return benchmark(spark, args)

def main(args: argparse.Namespace) -> None:
    appName = f'{args.which}_benchmark'
    # conf = SparkConf().setAll([('spark.executor.memory', args.executor), ('spark.driver.memory',args.driver)]) 
    spark = SparkSession.builder.appName(appName).getOrCreate()
    callSiteShortOrig = spark.sparkContext.getLocalProperty('callSite.short')

    result = None
    if(args.which == "column"): result = column_benchmark(spark, args)
    elif(args.which == "group_by"): result = group_by_benchmark(spark, args)
    elif(args.which == "nycdata"): result = nyc_benchmark(spark, args)
    else: raise Exception("Data not supported")

    spark.sparkContext.setLocalProperty('callSite.short', callSiteShortOrig)
    
    print("="*240)
    print('RESULTS')
    print(f'Test Run = {appName}, Operation = {str(args.list)}.')

    if args.outputfile is not None:
        output_name = f'output/{args.which}_{args.outputfile}'
        print(f'Writing results to {output_name}')
        print("="*240)
        
        schema = ['test']
        for i in range(args.ntest):
            schema.append(f'test_{i}')
        schema.append('mean_exclude_max_and_min')

        results_df = spark.createDataFrame(result, schema).coalesce(1)
        results_df.write.csv(
            output_name,
            header=True,
            mode='overwrite'
        )
        print(results_df.collect())

if __name__ == '__main__':
    args = arguments()
    print(args)
    main(args)
    