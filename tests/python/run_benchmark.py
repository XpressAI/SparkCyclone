from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark import SparkConf
from pyspark import StorageLevel

import argparse
import sys
from timeit import default_timer as timer

from column_operation_dict import operations, aggregate
from nyctaxi import queries

def arguments() -> argparse.Namespace:
    args = argparse.ArgumentParser(description='Run Benchmark. Please generate dataset using generate_data.py first.')
    args.add_argument('-x','--executor', type=str, default='4g', help='Set Executor Memory')
    args.add_argument('-d','--driver', type=str, default='4g', help='Set Driver Memory')
    args.add_argument('-sl','--storageLevel', type=str, default='11001', help='Set Storage Level')
    args.add_argument('-n','--ntest', type=int, default=5, help='Number of Tests')
    args.add_argument('-cc', '--clearcache', action='store_true', default=False, help="Clear cache for every tasks")
    args.add_argument('-o','--outputfile', type=str, default=None, help='Output file name (CSV)')
    subparsers = args.add_subparsers(help='Dataset Options', dest="which")

    random = subparsers.add_parser('random', help='Random Dataset')
    random.add_argument('inputfile', type=str, metavar='file_url', help='Input file URL')
    random.add_argument('-r','--repartitions', type=int, default=200, help='Number of partitions')
    random.add_argument('-t','--type', type=str, default='groupbyagg', help='Set Benchmark Type', choices=['groupbyagg','repart','innerjoin','broadinnerjoin', 'column'])
    random.add_argument('-l', '--list', help='Comma delimited list input', type=lambda s: [item for item in s.split(',')], default=None)
    
    nyc = subparsers.add_parser('nycdata', help='NYC-taxi-data Dataset')
    nyc.add_argument('-l', '--list', help='Comma delimited list input', type=lambda s: [item for item in s.split(',')], default=None)

    return args.parse_args()

def groupby_agg_benchmark(df: DataFrame, spark: SparkSession, args: argparse.Namespace) -> list:
    res = ['groupbyagg']

    for i in range(args.ntest):
        print("="*240)
        print('Starting Benchmark for GroupBy & Agg')
        spark.sparkContext.setLocalProperty('callSite.short', f'groupby_agg_benchmark()_test_{i}')
        
        if (args.clearcache):
            spark.catalog.clearCache()

        start_time = timer()
        res_df = (df.groupBy('prefix2').agg(
            F.count('*').alias('total_count'),
            F.countDistinct('prefix4').alias('prefix4_count'),
            F.countDistinct('prefix8').alias('prefix8_count'),
            F.sum('float_x').alias('float_val_sum'),
            F.sum('int_x').alias('integer_val_sum'),
        ))

        print(f'Count value for groupby_agg_benchmark() = {res_df.rdd.count()}')
        time_taken = timer() - start_time
        res_df.explain()
        print(f'Running for groupby_agg_benchmark_test_{i} = {time_taken}')
        res.append(time_taken)
    
    avg = (sum(res[1:]) - max(res[1:]) - min(res[1:])) / (args.ntest-2)
    print(f'Avg for groupby_agg_benchmark_test = {avg}')
    print("="*240)

    res.append(avg)
    
    return [tuple(res)]

def repartition_benchmark(df: DataFrame, spark: SparkSession, args: argparse.Namespace) -> list:
    res = ['repart']

    for i in range(args.ntest):
        print("="*240)
        print('Starting Benchmark for Repartition')
        spark.sparkContext.setLocalProperty('callSite.short', f'repartition_benchmark()_test_{i}')
        
        if (args.clearcache):
            spark.catalog.clearCache()

        start_time = timer()
        res_df = (df.repartition(args.repartitions,'prefix4'))

        print(f'Count value repartition_benchmark() = {res_df.rdd.count()}')
        print(f'Number of partition after repartition_benchmark() = {res_df.rdd.getNumPartitions()}')

        time_taken = timer() - start_time
        res_df.explain()
        print(f'Running for repartition_benchmark_{i} = {time_taken}')
        res.append(time_taken)
    
    avg = (sum(res[1:]) - max(res[1:]) - min(res[1:])) / (args.ntest-2)
    print(f'Avg for repartition_benchmark = {avg}')
    print("="*240)

    res.append(avg)

    return [tuple(res)]

def innerjoin_benchmark(df: DataFrame, spark: SparkSession, args: argparse.Namespace) -> list:
    res = ['innerjoin']

    for i in range(args.ntest):
        print("="*240)
        print('Starting Benchmark for Inner Join')
        spark.sparkContext.setLocalProperty('callSite.short', f'innerjoin_benchmark()_test_{i}')
        
        if (args.clearcache):
            spark.catalog.clearCache()

        start_time = timer()

        df1 = (df.groupBy('prefix2').agg(F.count('*').alias('total_count')))
        res_df = (df.join(df1, on='prefix2', how='inner'))

        print(f'Count value for innerjoin_benchmark() = {res_df.rdd.count()}')

        time_taken = timer() - start_time
        res_df.explain()
        print(f'Running for innerjoin_benchmark_{i} = {time_taken}')
        res.append(time_taken)

    avg = (sum(res[1:]) - max(res[1:]) - min(res[1:])) / (args.ntest-2)
    print(f'Avg for innerjoin_benchmark = {avg}')
    print("="*240)

    res.append(avg)
    
    return [tuple(res)]

def broadcast_innerjoin_benchmark(df: DataFrame, spark: SparkSession, args: argparse.Namespace) -> list:
    res = ['broadinnerjoin']
    
    for i in range(args.ntest):
        print("="*240)
        print('Starting Benchmark for Broadcast Inner Join')
        spark.sparkContext.setLocalProperty('callSite.short', f'broadcast_innerjoin_benchmark()_test_{i}')

        if (args.clearcache):
            spark.catalog.clearCache() 

        start_time = timer()

        df1 = (df.groupBy('prefix2').agg(F.count('*').alias('total_count')))
        res_df = (df.join(F.broadcast(df1), on='prefix2', how='inner'))

        print(f'Count value for broadcast_innerjoin_benchmark() = {res_df.rdd.count()}')

        time_taken = timer() - start_time
        res_df.explain()
        print(f'Running for broadcast_innerjoin_benchmark_{i} = {time_taken}')
        res.append(time_taken)

    avg = (sum(res[1:]) - max(res[1:]) - min(res[1:])) / (args.ntest-2)
    print(f'Avg for broadcast_innerjoin_benchmark = {avg}')
    print("="*240)

    res.append(avg)
    return [tuple(res)]
    
def column_benchmark(spark: SparkSession, args: argparse.Namespace, df: DataFrame = None) -> list:
    res = []
    dicts = None
    if(args.which == "random"): dicts = {**operations, **aggregate}
    elif(args.which == "nycdata"): dicts = queries
    else: raise Exception("Data not supported")
    
    ops = dicts.keys() if args.list is None else args.list
    
    print("="*240)
    print(f'Starting Benchmark for {args.which}_column_benchmark() : {str(ops)}')

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
                new_df = dicts[op](df) if args.which == "random" else dicts[op](spark)
                new_df.write.csv(
                    f'temp/{op}_{i}',
                    header=True,
                    mode='overwrite'
                )
                time_taken = timer() - start_time
                new_df.explain()
                print(f'Finished {op}_benchmark_test_{i} = {time_taken}')
                print("="*240)
                col_op.append(time_taken)

            avg = (sum(col_op[1:]) - max(col_op[1:]) - min(col_op[1:])) / (args.ntest-2)
            print(f'AVG for {op}_benchmark_test = {avg}')

            col_op.append(avg)
            res.append(tuple(col_op))

        except Exception as e:
            print(f'Error {str(e)}. Skipping operation {op}')
    
    print("="*240)

    return res

def parse_storage_level(level: list) -> list:
    for i in range(4):
        level[i] = bool(int(level[i]))
    level[4] = int(level[4])
    return level

def random_data(spark: SparkSession, args: argparse.Namespace) -> list:
    print("="*240)
    print(f'Shuffle Benchmark using input file = {args.inputfile} with storage level = {args.storageLevel}')
    print("="*240)


    schema = T.StructType([
        # T.StructField("value", T.StringType()),
        # T.StructField("prefix2", T.StringType()),
        # T.StructField("prefix4", T.StringType()),
        # T.StructField("prefix8", T.StringType()),
        T.StructField("float_x", T.DoubleType()),
        T.StructField("float_y", T.DoubleType()),
        T.StructField("int_x", T.LongType()),
        T.StructField("int_y", T.LongType()),
        T.StructField("float_a", T.DoubleType()),
        T.StructField("float_b", T.DoubleType()),
        T.StructField("int_a", T.LongType()),
        T.StructField("int_b", T.LongType()),
        # T.StructField("randn", T.LongType()),
        # T.StructField("randn1", T.DoubleType()),
        # T.StructField("degree", T.LongType()),
        # T.StructField("small_int", T.LongType()),
    ])

    level = parse_storage_level(list(tuple(args.storageLevel)))

    # initialize df
    df = None
    
    if "csv" in args.inputfile:
        df = spark.read.csv(args.inputfile, header=True, schema=schema).persist(StorageLevel(*tuple(level)))
    elif "json" in args.inputfile:
        df = spark.read.json(args.inputfile, schema=schema).persist(StorageLevel(*tuple(level)))
    elif "parquet" in args.inputfile:
        df = spark.read.parquet(args.inputfile, schema=schema).persist(StorageLevel(*tuple(level)))

    assert df, (f"Filetype not found in {args.inputfile}! Ensure that the path dir is correct.")

    result = None

    if(args.type == 'groupbyagg'):
        result = groupby_agg_benchmark(df, spark, args)

    elif(args.type == 'repart'):
        result = repartition_benchmark(df, spark, args)

    elif(args.type == 'innerjoin'):
        result = innerjoin_benchmark(df, spark, args)

    elif(args.type == 'broadinnerjoin'):
        result = broadcast_innerjoin_benchmark(df, spark, args)

    elif(args.type == 'column'):
        result = column_benchmark(spark, args, df=df)

    return result

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

    level = parse_storage_level(list(tuple(args.storageLevel)))
    
    df = spark.read.csv('data/trips_2020.csv', header=True, schema=schema_nyctaxi).persist(StorageLevel(*tuple(level)))
    df1 = spark.read.csv('data/cab_types.csv', header=True, schema=schema_cab).persist(StorageLevel(*tuple(level)))
    df.registerTempTable('trips')
    df1.registerTempTable('cab_types')
    df.printSchema()
    df1.printSchema()

    return column_benchmark(spark, args)

def main(args: argparse.Namespace) -> None:
    appName = f'{args.which}_benchmark'
    conf = SparkConf().setAll([('spark.executor.memory', args.executor), ('spark.driver.memory',args.driver)]) 
    spark = SparkSession.builder.appName(appName).config(conf=conf).getOrCreate()
    callSiteShortOrig = spark.sparkContext.getLocalProperty('callSite.short')

    result = None
    if(args.which == "random"): result = random_data(spark,args)
    elif(args.which == "nycdata"): result = nyc_benchmark(spark, args)
    else: raise Exception("Data not supported")

    spark.sparkContext.setLocalProperty('callSite.short', callSiteShortOrig)
    
    print("="*240)
    print('RESULTS')
    print(f'Test Run = {appName}, StorageLevel = {args.storageLevel}, Operation = {args.type if args.which == "random" else str(args.list)}.')

    if args.outputfile is not None:
        output_name = f'output/{args.which}_{args.outputfile}_{args.storageLevel}{"_" + args.type if args.which == "random" else ""}'
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
        results_df.show()

if __name__ == '__main__':
    args = arguments()
    print(args)
    main(args)
    