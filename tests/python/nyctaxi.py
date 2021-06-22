from pyspark.sql import SparkSession

def query_1(spark: SparkSession):
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

def query_2(spark: SparkSession):
    print("""Query: SELECT id, pickup_location_id,dropoff_location_id,payment_type, COUNT(*), AVG(fare_amount) 
        FROM trips group by id, pickup_location_id,dropoff_location_id,payment_type
    """)
    res = spark.sql('SELECT id, pickup_location_id,dropoff_location_id,payment_type, COUNT(*), AVG(fare_amount) \
        FROM trips group by id, pickup_location_id,dropoff_location_id,payment_type')
    return res

def query_3(spark: SparkSession):
    print("""Query: select id, pickup_location_id,dropoff_location_id,payment_type, COUNT(*), SUM(total_amount) 
        from trips group by id, pickup_location_id,dropoff_location_id,payment_type having SUM(fare_amount + extra) < 0
    """)
    res = spark.sql('select id, pickup_location_id,dropoff_location_id,payment_type, COUNT(*), SUM(total_amount) \
        from trips group by id, pickup_location_id,dropoff_location_id,payment_type having SUM(fare_amount + extra) < 0')
    return res

def query_4(spark: SparkSession):
    print("""Query: select trips.payment_type, trips.fare_amount, trips.mta_tax, trips.trip_distance, trips.tolls_amount, cab_types.type 
        from trips inner join cab_types on trips.cab_type_id = cab_types.id
    """)
    res = spark.sql('select trips.payment_type, trips.fare_amount, trips.mta_tax, trips.trip_distance, trips.tolls_amount, cab_types.type \
        from trips inner join cab_types on trips.cab_type_id = cab_types.id')
    return res

def query_5(spark: SparkSession):
    print("""Query: select corr(trip_distance, total_amount) as correlation, AVG(trip_distance)
    as mean_distance, AVG(total_amount) as mean_amount from trips
    """)
    res = spark.sql('select corr(trip_distance, total_amount) as correlation, AVG(trip_distance) as mean_distance, AVG(total_amount) as mean_amount from trips')
    return res

queries = {
    'q1': query_1, 
    'q2': query_2, 
    'q3': query_3, 
    'q4': query_4, 
    'q5': query_5
}
