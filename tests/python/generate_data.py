from pyspark.sql import SparkSession
import pyspark.sql.functions as Function
import pyspark.sql.types as Type

import argparse
import sys
import uuid

def arguments():
    args = argparse.ArgumentParser(description='Generates sample data in CSV / JSON / Parquet for PySpark Benchmark')
    args.add_argument('outfile', type=str, metavar='file_url', help='Output file URL')
    args.add_argument('-r','--rows', type=int, default=100, help='Number of rows')
    args.add_argument('-p','--partitions', type=int, default=100, help='Number of partitions')
    args.add_argument('-n','--name', type=str, default='Generete test data', help='PySpark job name')
    args.add_argument('-ft','--filetype', type=str, default='csv', help='Output file type. Supports csv, json, parquet')
    
    return args.parse_args()

def get_uuid():
    return uuid.uuid4().hex

def main(args):

    spark = SparkSession.builder.appName(args.name).getOrCreate()

    Logger = spark._jvm.org.apache.log4j.Logger
    log = Logger.getLogger(__name__)
    log.info('='*64)
    log.info('')
    log.info('Generating data with {0} rows, {1} partitions at {2}'.format(args.rows, args.partitions, args.outfile))
    log.info('')
    log.info('='*64)

    udfUUID = Function.udf(get_uuid, Type.StringType())

    df = (spark.range(0, args.rows, numPartitions=args.partitions)
        .withColumn('value', udfUUID())
        .withColumn('prefix2', Function.substring(Function.col('value'),1,2))
        .withColumn('prefix4', Function.substring(Function.col('value'),1,4))
        .withColumn('prefix8', Function.substring(Function.col('value'),1,8))
        .withColumn('float_val', Function.rand(seed=8675309)*1000000)
        .withColumn('integer_val', Function.col('float_val').cast(Type.LongType()))
        .drop('id'))


    output_folder =  args.outfile + "_R" + str(args.rows) + "_P" + str(args.partitions) + "_" + args.filetype 

    if args.filetype == "csv":
        df.write.csv(output_folder, mode='overwrite', header=True)
    elif args.filetype == "json":
        df.write.json(output_folder, mode='overwrite')
    elif args.filetype == "parquet":
        df.write.parquet(output_folder, mode='overwrite')

    log.info('Saved at {0}'.format(output_folder))

if __name__ == '__main__':
    args = arguments()
    main(args)
    