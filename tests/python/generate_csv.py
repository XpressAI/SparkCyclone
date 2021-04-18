from pyspark.sql import SparkSession
import pyspark.sql.functions as Function
import pyspark.sql.types as Type

import argparse
import sys
import uuid

def arguments():
    args = argparse.ArgumentParser(description='Generates sample data in form of CSV for PySpark Benchmark')
    args.add_argument('outfile', type=str, metavar='file_url', help='Output CSV file URL')
    args.add_argument('-r','--rows', type=int, default=100, help='Number of rows')
    args.add_argument('-p','--partitions', type=int, default=100, help='Number of partitions')
    args.add_argument('-n','--name', type=str, default='Generete test data', help='PySpark job name')
    
    return args.parse_args()

def getUUID():
    return uuid.uuid4().hex

def main():
    args = arguments()

    spark = SparkSession.builder.appName(args.name).getOrCreate()

    Logger = spark._jvm.org.apache.log4j.Logger
    Log = Logger.getLogger(__name__)
    Log.info('='*64)
    Log.info('')
    Log.info('Generating data with {0} rows, {1} partitions at {2}'.format(args.rows, args.partitions, args.outfile))
    Log.info('')
    Log.info('='*64)

    udfUUID = Function.udf(getUUID, Type.StringType())

    df = (spark.range(0, args.rows, numPartitions=args.partitions)
        .withColumn('value', udfUUID())
        .withColumn('prefix2', Function.substring(Function.col('value'),1,2))
        .withColumn('prefix4', Function.substring(Function.col('value'),1,4))
        .withColumn('prefix8', Function.substring(Function.col('value'),1,8))
        .withColumn('float_val', Function.rand(seed=8675309)*1000000)
        .withColumn('integer_val', Function.col('float_val').cast(Type.LongType()))
        .drop('id'))

    df.write.csv(args.outfile, mode='overwrite', header=True)

    Log.info('Saved at {0}'.format(args.outfile))

if __name__ == '__main__':
    main()