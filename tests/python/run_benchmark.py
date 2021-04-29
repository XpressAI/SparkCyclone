from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark import SparkConf
from pyspark import StorageLevel

import argparse
import sys
from timeit import default_timer as timer

def arguments():
    args = argparse.ArgumentParser(description='Run Benchmark. Please generate dataset using generate_data.py first.')
    args.add_argument('inputfile', type=str, metavar='file_url', help='Input file URL')
    args.add_argument('-r','--repartitions', type=int, default=200, help='Number of partitions')
    args.add_argument('-o','--outputfile', type=str, default=None, help='Output file name (CSV)')
    args.add_argument('-x','--executor', type=str, default='18g', help='Set Executor Memory')
    args.add_argument('-d','--driver', type=str, default='3g', help='Set Driver Memory')
    args.add_argument('-sl','--storageLevel', type=str, default='11001', help='Set Storage Level')
    args.add_argument('-t','--type', type=str, default='groupbyagg', help='Set Benchmark Type', choices=['groupbyagg','repart','innerjoin','broadinnerjoin', 'column'])

    return args.parse_args()

def groupby_agg_benchmark(df, log):
    log.info("="*64)
    log.info('Starting Benchmark for GroupBy & Agg')

    start_time = timer()
    res = (df.groupBy('prefix2').agg(
        F.count('*').alias('total_count'),
        F.countDistinct('prefix4').alias('prefix4_count'),
        F.countDistinct('prefix8').alias('prefix8_count'),
        F.sum('float_val').alias('float_val_sum'),
        F.sum('integer_val').alias('integer_val_sum'),
    ))

    count = res.rdd.count()

    log.info(f'Count value for groupby_agg_benchmark() = {count}')
    log.info("="*64)
    return timer() - start_time

def repartition_benchmark(df, partitions, log):
    log.info("="*64)
    log.info('Starting Benchmark for Repartition')

    start_time = timer()
    res = (df.repartition(partitions,'prefix4'))

    count = res.rdd.count()

    log.info(f'Count value repartition_benchmark() = {count}')
    log.info(f'Number of partition after repartition_benchmark() = {res.rdd.getNumPartitions()}')
    log.info("="*64)

    return timer() - start_time

def innerjoin_benchmark(df, log):
    log.info("="*64)
    log.info('Starting Benchmark for Inner Join')

    start_time = timer()

    df1 = (df.groupBy('prefix2').agg(F.count('*').alias('total_count')))
    res = (df.join(df1, on='prefix2', how='inner'))

    count = res.rdd.count()

    log.info(f'Count value for innerjoin_benchmark() = {count}')
    log.info("="*64)

    return timer() - start_time

def broadcast_innerjoin_benchmark(df, log):
    log.info("="*64)
    log.info('Starting Benchmark for Broadcast Inner Join')

    start_time = timer()

    df1 = (df.groupBy('prefix2').agg(F.count('*').alias('total_count')))
    res = (df.join(F.broadcast(df1), on='prefix2', how='inner'))

    count = res.rdd.count()

    log.info(f'Count value for broadcast_innerjoin_benchmark() = {count}')
    log.info("="*64)

    return timer() - start_time
    
def column_benchmark(df,log):

    start_time = timer()
    log.info("="*64)
    log.info('Starting Benchmark for column_benchmark()')

    df = df.withColumn("randn", (F.randn()*10).cast(T.LongType())) \
                    .withColumn("randn1", F.randn()) \
                    .withColumn('degree', (F.randn()*360).cast(T.LongType())) \
                    .withColumn('small_int', (F.rand()*10).cast(T.LongType())) \
                    .withColumn('sum_two_cols', df['float_val'] + df['integer_val']) \
                    .withColumn('subtract_two_cols', df['float_val'] - df['integer_val']) \
                    .withColumn('mul_two_cols', df['float_val'] * df['integer_val']) \
                    .withColumn('div_two_cols', df['float_val'] / df['integer_val']) 
    df = df.select('*', 
                   F.abs(df['integer_val']), F.acos(df['randn']),  F.acosh(df['randn']), 
                   F.ascii(df['prefix2']), F.asin(df['randn1']), F.asinh(df['randn1']), 
                   F.atan(df['randn1']), F.atanh(df['randn1']), F.atan2(df['randn1'], df['randn']), 
                   F.base64(df['value']), F.bin(df['integer_val']), F.bitwiseNOT(df['integer_val']), 
                   F.cbrt(df['randn']), F.concat(df['prefix2'], df['prefix4']), 
                   F.concat_ws('-', df['prefix2'], df['prefix4'], df['float_val']), 
                   F.conv(df['integer_val'], 10, 16), F.cos(df['randn']), F.cosh(df['randn']), 
                   F.crc32(df['value']), F.degrees(df['degree']), F.exp(df['randn']), 
                   F.expr("length(float_val)"), F.factorial(df['small_int']), F.hash(df['value']), 
                   F.hex(df['value']), F.hypot(df['integer_val'], df['randn']), F.levenshtein(df['value'],df['integer_val']), 
                   F.log(df['integer_val']), F.log10(df['float_val']), F.log1p(df['randn']), 
                   F.log2(df['randn1']), F.md5(df['value']), F.pow(df['randn'], df['small_int']), 
                   F.radians(df['degree']), F.sha1(df['value']), F.sha2(df['value'], 256), 
                   F.signum(df['integer_val']), F.sin(df['randn']), F.sinh(df['randn']), 
                   F.sqrt(df['small_int']), F.tan(df['randn']), F.tanh(df['randn']), 
                   F.xxhash64(df['value']), df['small_int'].bitwiseAND(df['integer_val']), 
                   df['small_int'].bitwiseOR(df['integer_val']), df['small_int'].bitwiseXOR(df['integer_val']))
    
    df_agg = df.agg(F.approx_count_distinct(df.integer_val), 
                    F.avg(df.integer_val), F.corr(df['float_val'], df['randn']), 
                    F.count(df['value']), F.countDistinct(df['value'], df['integer_val']), 
                    F.covar_pop(df['integer_val'], df['float_val']), F.covar_samp(df['randn'], df['float_val']), 
                    F.kurtosis(df['randn1']), F.max(df['float_val']), F.mean(df['randn']), 
                    F.min(df['randn']), F.percentile_approx('randn',[0.25,0.5,0.75], 100000), 
                    F.skewness(df['randn']), F.stddev(df['randn1']), F.stddev_pop(df['randn1']), 
                    F.stddev_samp(df['randn1']), F.sum(df['integer_val']), 
                    F.sumDistinct(df['integer_val']), F.var_pop(df['small_int']), F.var_samp(df['integer_val']))
    
    count, count_agg = (df.rdd.count(), len(df.columns)), len(df_agg.columns)
    
    log.info(f'Shape of new DF = {count}, total columns in df_agg = {count_agg}')
    log.info("="*64)

    return timer() - start_time, df, df_agg

def main(args):
    appName = f'{args.type}_benchmark'
    conf = SparkConf().setAll([('spark.executor.memory', args.executor), ('spark.driver.memory',args.driver)]) 
    spark = SparkSession.builder.appName(appName).config(conf=conf).getOrCreate()

    logger = spark._jvm.org.apache.log4j.Logger
    log = logger.getLogger(__name__)

    log.info("="*64)
    log.info(f'Shuffle Benchmark using input file = {args.inputfile} with storage level = {args.storageLevel}')
    log.info("="*64)

    callSiteShortOrig = spark.sparkContext.getLocalProperty('callSite.short')

    schema = T.StructType([
        T.StructField("value", T.StringType()),
        T.StructField("prefix2", T.StringType()),
        T.StructField("prefix4", T.StringType()),
        T.StructField("prefix8", T.StringType()),
        T.StructField("float_val", T.DoubleType()),
        T.StructField("integer_val", T.LongType())
    ])

    level = list(tuple(args.storageLevel))
    for i in range(4):
        level[i] = bool(int(level[i]))
    level[4] = int(level[4])


    # initialize df
    df = None
    
    if "csv" in args.inputfile:
        df = spark.read.csv(args.inputfile, header=True, schema=schema).persist(StorageLevel(*tuple(level)))
    elif "json" in args.inputfile:
        df = spark.read.json(args.inputfile, schema=schema).persist(StorageLevel(*tuple(level)))
    elif "parquet" in args.inputfile:
        df = spark.read.parquet(args.inputfile, schema=schema).persist(StorageLevel(*tuple(level)))

    assert df, (f"Filetype not found in {args.inputfile}! Ensure that the path dir is correct.")

    TEST = 5
    result = []
    for i in range(TEST):
        if(args.type == 'groupbyagg'):
            spark.sparkContext.setLocalProperty('callSite.short', f'groupby_agg_benchmark()_test_{i}')
            groupby_agg_benchmark_time = groupby_agg_benchmark(df, log)
            result.append(groupby_agg_benchmark_time)

        elif(args.type == 'repart'):
            spark.sparkContext.setLocalProperty('callSite.short', f'repartition_benchmark()_test_{i}')
            repartition_benchmark_time = repartition_benchmark(df, args.repartitions, log)
            result.append(repartition_benchmark_time)

        elif(args.type == 'innerjoin'):
            spark.sparkContext.setLocalProperty('callSite.short', f'innerjoin_benchmark()_test_{i}')
            innerjoin_benchmark_time = innerjoin_benchmark(df, log)
            result.append(innerjoin_benchmark_time)

        elif(args.type == 'broadinnerjoin'):
            spark.sparkContext.setLocalProperty('callSite.short', f'broadcast_innerjoin_benchmark()_test_{i}')
            broadcast_innerjoin_benchmark_time = broadcast_innerjoin_benchmark(df, log)
            result.append(broadcast_innerjoin_benchmark_time)

        elif(args.type == 'column'):
            spark.sparkContext.setLocalProperty('callSite.short', f'column_benchmark()_test_{i}')
            column_benchmark_time, _, _ = column_benchmark(df, log)
            result.append(column_benchmark_time)

    spark.sparkContext.setLocalProperty('callSite.short', callSiteShortOrig)
    
    average = 0
    log.info("="*64)
    log.info('RESULTS')
    log.info(f'Test Run = {appName}, StorageLevel = {args.storageLevel}, Operation = {args.type}.')
    
    for i in range(TEST):
        log.info(f'TEST {i} test time : {result[i]} s')
        if i > 0 and i < 4:
            average += result[i]

    log.info(f'Average for TEST 1:3 : {average/3} s')
    log.info("="*64)

    if args.outputfile is not None:
        log.info(f'Writing results to {args.outputfile}_{args.storageLevel}_{args.type}')
        
        result.insert(0, args.type)
        result.insert(len(result), average/3)
        results_list = [tuple(result)]
    
        results_schema = T.StructType([
            T.StructField("test", T.StringType()),
            T.StructField("test_0", T.DoubleType()),
            T.StructField("test_1", T.DoubleType()),
            T.StructField("test_2", T.DoubleType()),
            T.StructField("test_3", T.DoubleType()),
            T.StructField("test_4", T.DoubleType()),
            T.StructField("average 1:3", T.DoubleType())
        ])
        results_df = spark.createDataFrame(results_list, schema=results_schema).coalesce(1)
        results_df.write.csv(
            f'{args.outputfile}_{args.storageLevel}_{args.type}',
            header=True,
            mode='overwrite'
        )

if __name__ == '__main__':
    args = arguments()
    main(args)
    