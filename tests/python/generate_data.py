from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

import argparse
import sys
# import uuid

def arguments():
    args = argparse.ArgumentParser(description='Generates sample data in CSV / JSON / Parquet for PySpark Benchmark')
    args.add_argument('outfile', type=str, metavar='file_url', help='Output file URL')
    args.add_argument('-r','--rows', type=int, default=100, help='Number of rows')
    args.add_argument('-p','--partitions', type=int, default=100, help='Number of partitions')
    args.add_argument('-n','--name', type=str, default='Generete test data', help='PySpark job name')
    args.add_argument('-ft','--filetype', type=str, default='csv', help='Output file type. Supports csv, json, parquet', choices=['csv', 'parquet', 'json'])
    
    return args.parse_args()

# def get_uuid():
#     return uuid.uuid4().hex

def main(args):

    spark = SparkSession.builder.appName(args.name).getOrCreate()

    print('='*64)
    print(f'Generating data with {args.rows} rows, {args.partitions} partitions at {args.outfile}')
    print('='*64)

    # udfUUID = F.udf(get_uuid, T.StringType())

    df = (spark.range(0, args.rows, numPartitions=args.partitions)
        # .withColumn('value', udfUUID())
        # .withColumn('prefix2', F.substring(F.col('value'),1,2))
        # .withColumn('prefix4', F.substring(F.col('value'),1,4))
        # .withColumn('prefix8', F.substring(F.col('value'),1,8))
        .drop('id')
        .withColumn('id', F.floor(F.rand()*(args.rows/10)).cast(T.DoubleType()))
        .withColumn('double_x', F.rand(seed=8675309)*100000)
        .withColumn('double_y', F.rand(seed=8675367)*10000)
        #.withColumn('int_x', F.col('double_x').cast(T.LongType()))
        #.withColumn('int_y', F.col('double_y').cast(T.LongType()))
        # .withColumn('double_a', F.rand(seed=8675309)*1000)
        #.withColumn('double_b', F.rand(seed=8675367)*100)
        #.withColumn('int_a', F.col('double_a').cast(T.LongType()))
        #.withColumn('int_b', F.col('double_b').cast(T.LongType()))
        # .withColumn("randn", (F.randn()*10).cast(T.LongType())) 
        # .withColumn("randn1", F.randn()) 
        # .withColumn('degree', (F.randn()*360).cast(T.LongType())) 
        # .withColumn('small_int', (F.rand()*10).cast(T.LongType()))
    )


    output_folder =  args.outfile + "_R" + str(args.rows) + "_P" + str(args.partitions) + "_" + args.filetype 

    if args.filetype == "csv":
        df.write.csv(output_folder, mode='overwrite', header=True)
    elif args.filetype == "json":
        df.write.json(output_folder, mode='overwrite')
    elif args.filetype == "parquet":
        df.write.parquet(output_folder, mode='overwrite')

    print(f'Saved at {output_folder}')

if __name__ == '__main__':
    args = arguments()
    main(args)
    