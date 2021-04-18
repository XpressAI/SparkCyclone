from pyspark.sql import SparkSession
import pyspark.sql.functions as Function
import pyspark.sql.types as Type
from pyspark import SparkConf
from pyspark import StorageLevel

import argparse
import sys
from timeit import default_timer as timer

def arguments():
    args = argparse.ArgumentParser(description='Shuffle Benchmark. Please generate dataset using generate_csv.py first.')
    args.add_argument('inputfile', type=str, metavar='file_url', help='Input CSV file URL')
    args.add_argument('-n','--name', type=str, default='shuffle_benchmark', help='PySpark job name')
    args.add_argument('-r','--repartitions', type=int, default=200, help='Number of partitions')
    args.add_argument('-o','--outputfile', type=str, default=None, help='Output file name (CSV')
    args.add_argument('-x','--executor', type=str, default='18g', help='Set Executor Memory')
    args.add_argument('-d','--driver', type=str, default='3g', help='Set Driver Memory')
    args.add_argument('-sl','--storageLevel', type=str, default='11001', help='Set Storage Level')
    args.add_argument('-t','--type', type=str, default='groupbyagg', help='Set Shuffle Benchmark Type', choices=['groupbyagg','repart','innerjoin','broadinnerjoin'])

    return args.parse_args()

def groupByAggBenchmark(df, Log):
    Log.info("="*64)
    Log.info('Starting Benchmark for GroupBy & Agg')

    start_time = timer()
    res = (df.groupBy('prefix2').agg(
        Function.count('*').alias('total_count'),
        Function.countDistinct('prefix4').alias('prefix4_count'),
        Function.countDistinct('prefix8').alias('prefix8_count'),
        Function.sum('float_val').alias('float_val_sum'),
        Function.sum('integer_val').alias('integer_val_sum'),
    ))

    count = res.rdd.count()

    Log.info('Count value for groupByAggBenchmark() = {0}'.format(count))
    Log.info("="*64)
    return timer() - start_time

def repartitionBenchmark(df, partitions, Log):
    Log.info("="*64)
    Log.info('Starting Benchmark for Repartition')

    start_time = timer()
    res = (df.repartition(partitions,'prefix4'))

    count = res.rdd.count()

    Log.info('Count value repartitionBenchmark() = {0}'.format(count))
    Log.info('Number of partition after repartitionBenchmark() = {0}'.format(res.rdd.getNumPartitions()))
    Log.info("="*64)

    return timer() - start_time

def innerJoinBenchmark(df, Log):
    Log.info("="*64)
    Log.info('Starting Benchmark for Inner Join')

    start_time = timer()

    df1 = (df.groupBy('prefix2').agg(Function.count('*').alias('total_count')))
    res = (df.join(df1, on='prefix2', how='inner'))

    count = res.rdd.count()

    Log.info('Count value for innerJoinBenchmark() = {0}'.format(count))
    Log.info("="*64)

    return timer() - start_time

def broadcastInnerJoinBenchmark(df, Log):
    Log.info("="*64)
    Log.info('Starting Benchmark for Broadcast Inner Join')

    start_time = timer()

    df1 = (df.groupBy('prefix2').agg(Function.count('*').alias('total_count')))
    res = (df.join(Function.broadcast(df1), on='prefix2', how='inner'))

    count = res.rdd.count()

    Log.info('Count value for broadcastInnerJoinBenchmark() = {0}'.format(count))
    Log.info("="*64)

    return timer() - start_time

def main():
    args = arguments()

    conf = SparkConf().setAll([('spark.executor.memory', args.executor), ('spark.driver.memory',args.driver)]) 
    spark = SparkSession.builder.appName(args.name).config(conf=conf).getOrCreate()

    Logger = spark._jvm.org.apache.log4j.Logger
    Log = Logger.getLogger(__name__)

    Log.info("="*64)
    Log.info('Shuffle Benchmark using input file = {0} with storage level = {1}'.format(args.inputfile, args.storageLevel))
    Log.info("="*64)

    callSiteShortOrig = spark.sparkContext.getLocalProperty('callSite.short')
    callSiteLongOrig = spark.sparkContext.getLocalProperty('callSite.long')

    schema = Type.StructType([
        Type.StructField("value", Type.StringType()),
        Type.StructField("prefix2", Type.StringType()),
        Type.StructField("prefix4", Type.StringType()),
        Type.StructField("prefix8", Type.StringType()),
        Type.StructField("float_val", Type.DoubleType()),
        Type.StructField("integer_val", Type.LongType())
    ])

    level = list(tuple(args.storageLevel))
    for i in range(4):
        level[i] = bool(int(level[i]))
    level[4] = int(level[4])
    df = spark.read.csv(args.inputfile, header=True, schema=schema).persist(StorageLevel(*tuple(level)))

    TEST = 5
    result = []
    for i in range(TEST):
        if(args.type == 'groupbyagg'):
            spark.sparkContext.setLocalProperty('callSite.short', f'groupByAggBenchmark()_test_{i}')
            spark.sparkContext.setLocalProperty('callSite.long', f'Benchmark for performing groupByAggBenchmark() on a dataframe for test {i}')
            groupByAggBenchmark_time = groupByAggBenchmark(df, Log)
            result.append(groupByAggBenchmark_time)

        elif(args.type == 'repart'):
            spark.sparkContext.setLocalProperty('callSite.short', f'repartitionBenchmark()_test_{i}')
            spark.sparkContext.setLocalProperty('callSite.long', f'Benchmark for repartitionBenchmark() a dataframe for test {i}')
            repartitionBenchmark_time = repartitionBenchmark(df, args.repartitions, Log)
            result.append(repartitionBenchmark_time)

        elif(args.type == 'innerjoin'):
            spark.sparkContext.setLocalProperty('callSite.short', f'innerJoinBenchmark()_test_{i}')
            spark.sparkContext.setLocalProperty('callSite.long', f'Benchmark for performing innerJoinBenchmark() for test {i}')
            innerJoinBenchmark_time = innerJoinBenchmark(df, Log)
            result.append(innerJoinBenchmark_time)

        elif(args.type == 'broadinnerjoin'):
            spark.sparkContext.setLocalProperty('callSite.short', f'broadcastInnerJoinBenchmark()_test_{i}')
            spark.sparkContext.setLocalProperty('callSite.long', f'Benchmark for performing broadcastInnerJoinBenchmark() for test {i}')
            broadcastInnerJoinBenchmark_time = broadcastInnerJoinBenchmark(df, Log)
            result.append(broadcastInnerJoinBenchmark_time)

    spark.sparkContext.setLocalProperty('callSite.short', callSiteShortOrig)
    spark.sparkContext.setLocalProperty('callSite.long', callSiteLongOrig)
    
    average = 0
    Log.info("="*64)
    Log.info('RESULTS')
    Log.info('Test Run = {0}, StorageLevel = {1}, Operation = {2}.'.format(args.name, args.storageLevel, args.type))
    
    for i in range(TEST):
        Log.info(f'TEST {i} test time : {result[i]} s')
        if i > 0 and i < 4:
            average += result[i]

    Log.info(f'Average for TEST 1:3 : {average/3} s')
    Log.info("="*64)

    if args.outputfile is not None:
        Log.info(f'Writing results to {args.outputfile}_{args.storageLevel}_{args.type}')
        
        result.insert(0, args.type)
        result.insert(len(result), average/3)
        results_list = [tuple(result)]
    
        results_schema = Type.StructType([
            Type.StructField("test", Type.StringType()),
            Type.StructField("test_0", Type.DoubleType()),
            Type.StructField("test_1", Type.DoubleType()),
            Type.StructField("test_2", Type.DoubleType()),
            Type.StructField("test_3", Type.DoubleType()),
            Type.StructField("test_4", Type.DoubleType()),
            Type.StructField("average 1:3", Type.DoubleType())
        ])
        results_df = spark.createDataFrame(results_list, schema=results_schema).coalesce(1)
        results_df.write.csv(
            f'{args.outputfile}_{args.storageLevel}_{args.type}',
            header=True,
            mode='overwrite'
        )

if __name__ == '__main__':
    main()