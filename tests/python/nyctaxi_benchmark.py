from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark import SparkConf, StorageLevel

import argparse
from timeit import default_timer as timer

def query_1(spark):
    print("""Query: SELECT id,pickup_datetime,dropoff_datetime,fare_amount,pickup_location_id,dropoff_location_id 
        FROM trips WHERE payment_type = 2 
        group by id,pickup_datetime,dropoff_datetime,fare_amount,pickup_location_id,dropoff_location_id 
        having fare_amount > 20.0
        """)
    res = spark.sql('SELECT id,pickup_datetime,dropoff_datetime,fare_amount,pickup_location_id,dropoff_location_id \
        FROM trips WHERE payment_type = 2 \
        group by id,pickup_datetime,dropoff_datetime,fare_amount,pickup_location_id,dropoff_location_id \
        having fare_amount > 20.0')
    return res

def query_2(spark):
    print("""Query: SELECT id, pickup_location_id,dropoff_location_id,payment_type, COUNT(*), AVG(fare_amount) 
        FROM trips group by id, pickup_location_id,dropoff_location_id,payment_type
    """)
    res = spark.sql('SELECT id, pickup_location_id,dropoff_location_id,payment_type, COUNT(*), AVG(fare_amount) \
        FROM trips group by id, pickup_location_id,dropoff_location_id,payment_type')
    return res

def query_3(spark):
    print("""Query: select id, pickup_location_id,dropoff_location_id,payment_type, COUNT(*), SUM(total_amount) 
        from trips group by id, pickup_location_id,dropoff_location_id,payment_type having SUM(fare_amount + extra) < 0
    """)
    res = spark.sql('select id, pickup_location_id,dropoff_location_id,payment_type, COUNT(*), SUM(total_amount) \
        from trips group by id, pickup_location_id,dropoff_location_id,payment_type having SUM(fare_amount + extra) < 0')
    return res

def query_4(spark):
    print("""Query: select trips.payment_type, trips.fare_amount, trips.mta_tax, trips.trip_distance, trips.tolls_amount, cab_types.type 
        from trips inner join cab_types on trips.cab_type_id = cab_types.id
    """)
    res = spark.sql('select trips.payment_type, trips.fare_amount, trips.mta_tax, trips.trip_distance, trips.tolls_amount, cab_types.type \
        from trips inner join cab_types on trips.cab_type_id = cab_types.id')
    return res

def query_5(spark):
    print("""Query: select corr(trip_distance, total_amount) as correlation, AVG(trip_distance)
    as mean_distance, AVG(total_amount) as mean_amount from trips
    """)
    res = spark.sql('select corr(trip_distance, total_amount) as correlation, AVG(trip_distance) as mean_distance, AVG(total_amount) as mean_amount from trips')
    return res

if __name__ == '__main__':
    conf = SparkConf().setAll([('spark.executor.memory', '4g'), ('spark.driver.memory', '4g')]) 
    spark = SparkSession.builder.appName('NYC').config(conf=conf).getOrCreate()
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

    df = spark.read.csv('data/trips_2020.csv', header=True, schema=schema_nyctaxi).persist(StorageLevel.MEMORY_AND_DISK)
    df1 = spark.read.csv('data/cab_types.csv', header=True, schema=schema_cab).persist(StorageLevel.MEMORY_AND_DISK)
    df.registerTempTable('trips')
    df1.registerTempTable('cab_types')
    print(f'{df.schema} \n{df1.schema}')
    # print(df.show(5))
    # print(df1.show(5))

    queries = {
        'q1': query_1, 
        'q2': query_2, 
        'q3': query_3, 
        'q4': query_4, 
        'q5': query_5
    }
    res = []

    for op in queries:
        col_op = [op]

        try:

            for i in range(5):
                print("="*240)
                print(f'Running {op}_benchmark_test_{i}')

                spark.catalog.clearCache() 

                start_time = timer()
                new_df = queries[op](spark)
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

            avg = (sum(col_op[1:]) - max(col_op[1:]) - min(col_op[1:])) / (5-2)
            print(f'AVG for {op}_benchmark_test = {avg}')

            col_op.append(avg)
            res.append(tuple(col_op))
        
        except Exception as e:
            print(f'Error {str(e)}. Skipping operation {op}')
    
    schema = ['test']
    for i in range(5):
        schema.append(f'test_{i}')
    schema.append('mean_exclude_max_and_min')

    results_df = spark.createDataFrame(res, schema).coalesce(1)
    results_df.write.csv(
        f'res',
        header=True,
        mode='overwrite'
    )
    print(results_df.show())
