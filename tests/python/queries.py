import pyspark.sql.functions as F
import pyspark.sql.types as T
from typing import Callable
from pyspark.sql import SparkSession, DataFrame

# helper function for looping 
def loop(op: Callable[[DataFrame], DataFrame], df: DataFrame = None) -> DataFrame:
    for _ in range(10):
        df = op(df)
    return df

class query(object):
    def __getitem__(self, name):
        return getattr(self, name)
    
class column_queries(query):
    
    def avg_x_double(self, spark: SparkSession) -> DataFrame:
        print("Query: SELECT AVG(double_x) FROM table")
        return spark.sql('SELECT AVG(double_x) FROM table')

    def avg_x_plus_y_double(self, spark: SparkSession) -> DataFrame:
        print("Query: SELECT AVG(double_x + double_y) FROM table")
        return spark.sql('SELECT AVG(double_x + double_y) FROM table')
    
    def sum_x_double(self, spark: SparkSession) -> DataFrame:
        print("Query: SELECT SUM(double_x) FROM table")
        return spark.sql('SELECT SUM(double_x) FROM table')

    def sum_x_plus_y_double(self, spark: SparkSession) -> DataFrame:
        print("Query: SELECT SUM(double_x + double_y) FROM table")
        return spark.sql('SELECT SUM(double_x + double_y) FROM table')
    
    def x_plus_y_double(self, spark: SparkSession) -> DataFrame:
        print("Query: SELECT double_x + double_y FROM table")
        return spark.sql('SELECT double_x + double_y FROM table') 


class group_by_queries(query):
    
    def group_by_sum_x(self, spark: SparkSession) -> DataFrame:
        print("Query: SELECT id, SUM(double_x) FROM table GROUP BY id")
        return spark.sql('SELECT id, SUM(double_x) FROM table GROUP BY id')

    def group_by_avg_x(self, spark: SparkSession) -> DataFrame:
        print("Query: SELECT id, AVG(double_x) FROM table GROUP BY id")
        return spark.sql('SELECT id, AVG(double_x) FROM table GROUP BY id')

    def group_by_sum_x_plus_y(self, spark: SparkSession) -> DataFrame:
        print("Query: SELECT id, SUM(double_x + double_y) FROM table GROUP BY id")
        return spark.sql('SELECT id, SUM(double_x + double_y) FROM table GROUP BY id')

    def group_by_avg_x_plus_y(self, spark: SparkSession) -> DataFrame:
        print("Query: SELECT id, AVG(double_x + double_y) FROM table GROUP BY id")
        return spark.sql('SELECT id, AVG(double_x + double_y) FROM table GROUP BY id')

    def group_by_count_asterisk(self, spark: SparkSession) -> DataFrame:
        print("Query: SELECT id, COUNT(*) FROM table GROUP BY id")
        return spark.sql('SELECT id, COUNT(*) FROM table GROUP BY id')

    def group_by_sum_x_minus_y(self, spark: SparkSession) -> DataFrame:
        print("Query: SELECT id, SUM(double_x - double_y) FROM table GROUP BY id")
        return spark.sql('SELECT id, SUM(double_x - double_y) FROM table GROUP BY id')

    def group_by_sum_x_plus_y_as_res_filter_res(self, spark: SparkSession) -> DataFrame:
        print("Query: SELECT id, SUM(double_x + double_y) AS res FROM table GROUP BY id HAVING res > 405008")
        return spark.sql("SELECT id, SUM(double_x + double_y) AS res FROM table GROUP BY id HAVING res > 405008")

class nyctaxi_queries(query):
    
    def q1(self, spark: SparkSession) -> DataFrame:
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

    def q2(self, spark: SparkSession) -> DataFrame:
        print("""Query: SELECT id, pickup_location_id,dropoff_location_id,payment_type, COUNT(*), AVG(fare_amount) 
            FROM trips group by id, pickup_location_id,dropoff_location_id,payment_type
        """)
        res = spark.sql('SELECT id, pickup_location_id,dropoff_location_id,payment_type, COUNT(*), AVG(fare_amount) \
            FROM trips group by id, pickup_location_id,dropoff_location_id,payment_type')
        return res

    def q3(self, spark: SparkSession) -> DataFrame:
        print("""Query: select id, pickup_location_id,dropoff_location_id,payment_type, COUNT(*), SUM(total_amount) 
            from trips group by id, pickup_location_id,dropoff_location_id,payment_type having SUM(fare_amount + extra) < 0
        """)
        res = spark.sql('select id, pickup_location_id,dropoff_location_id,payment_type, COUNT(*), SUM(total_amount) \
            from trips group by id, pickup_location_id,dropoff_location_id,payment_type having SUM(fare_amount + extra) < 0')
        return res

    def q4(self, spark: SparkSession) -> DataFrame:
        print("""Query: select trips.payment_type, trips.fare_amount, trips.mta_tax, trips.trip_distance, trips.tolls_amount, cab_types.type 
            from trips inner join cab_types on trips.cab_type_id = cab_types.id
        """)
        res = spark.sql('select trips.payment_type, trips.fare_amount, trips.mta_tax, trips.trip_distance, trips.tolls_amount, cab_types.type \
            from trips inner join cab_types on trips.cab_type_id = cab_types.id')
        return res

    def q5(self, spark: SparkSession) -> DataFrame:
        print("""Query: select corr(trip_distance, total_amount) as correlation, AVG(trip_distance)
        as mean_distance, AVG(total_amount) as mean_amount from trips
        """)
        res = spark.sql('select corr(trip_distance, total_amount) as correlation, AVG(trip_distance) as mean_distance, AVG(total_amount) as mean_amount from trips')
        return res

# operations = {
#     'abs' : lambda df : df.select(F.abs(df['int_x'])),
#     'acos' : lambda df : df.select(F.acos(df['randn'])),
#     'acosh' : lambda df : df.select(F.acosh(df['randn'])),
#     'ascii': lambda df : df.select(F.ascii(df['prefix2'])),
#     'asin': lambda df : df.select(F.asin(df['randn1'])),
#     'asinh' : lambda df : df.select(F.asinh(df['randn1'])), 
#     'atan' : lambda df : df.select(F.atan(df['randn1'])),
#     'atanh' : lambda df : df.select(F.atanh(df['randn1'])),
#     'atan2' : lambda df : df.select(F.atan2(df['randn1'], df['randn'])),
#     'base64' : lambda df : df.select(F.base64(df['value'])), 
#     'bin' : lambda df : df.select(F.bin(df['int_x'])),
#     'bitwiseNOT' : lambda df : df.select(F.bitwiseNOT(df['int_x'])), 
#     'cbrt' : lambda df : df.select(F.cbrt(df['randn'])),
#     'concat' : lambda df : df.select(F.concat(df['prefix2'], df['prefix4'])), 
#     'concat_ws' : lambda df : df.select(F.concat_ws('-', df['prefix2'], df['prefix4'], df['double_x'])),
#     'conv' : lambda df : df.select(F.conv(df['int_x'], 10, 16)),
#     'cos' : lambda df : df.select(F.cos(df['randn'])),
#     'cosh' : lambda df : df.select(F.cosh(df['randn'])), 
#     'crc32' : lambda df : df.select(F.crc32(df['value'])),
#     'degrees' : lambda df : df.select(F.degrees(df['degree'])),
#     'exp' : lambda df : df.select(F.exp(df['randn'])), 
#     'expr' : lambda df : df.select(F.expr("length(double_x)")),
#     'factorial' : lambda df : df.select(F.factorial(df['small_int'])),
#     'hash' : lambda df : df.select(F.hash(df['value'])), 
#     'hex' : lambda df : df.select(F.hex(df['value'])),
#     'hypot' : lambda df : df.select(F.hypot(df['int_x'], df['randn'])),
#     'levenshtein' : lambda df : df.select(F.levenshtein(df['value'],df['int_x'])), 
#     'log' : lambda df : df.select(F.log(df['int_x'])),
#     'log10' : lambda df : df.select(F.log10(df['double_x'])),
#     'log1p' : lambda df : df.select(F.log1p(df['randn'])), 
#     'log2' : lambda df : df.select(F.log2(df['randn1'])),
#     'md5' : lambda df : df.select(F.md5(df['value'])),
#     'pow' : lambda df : df.select(F.pow(df['randn'], df['small_int'])),
#     'radians' : lambda df : df.select(F.radians(df['degree'])),
#     'sha1' : lambda df : df.select(F.sha1(df['value'])),
#     'sha2' : lambda df : df.select(F.sha2(df['value'], 256)),
#     'signum' : lambda df : df.select(F.signum(df['int_x'])),
#     'sin' : lambda df : df.select(F.sin(df['randn'])),
#     'sinh' : lambda df : df.select(F.sinh(df['randn'])),
#     'sqrt' : lambda df : df.select(F.sqrt(df['small_int'])),
#     'tan' : lambda df : df.select(F.tan(df['randn'])),
#     'tanh' : lambda df : df.select(F.tanh(df['randn'])),
#     'xxhash64' : lambda df : df.select(F.xxhash64(df['value'])),
#     'bitwiseAND' : lambda df : df.select(df['small_int'].bitwiseAND(df['int_x'])),
#     'bitwiseOR' : lambda df : df.select(df['small_int'].bitwiseOR(df['int_x'])),
#     'bitwiseXOR' : lambda df : df.select(df['small_int'].bitwiseXOR(df['int_x'])),
#     '(x+y)_double': lambda df : df.select(df['double_x'] + df['double_y']),
#     '(x-y)_double': lambda df : df.select(df['double_x'] - df['double_y']),
#     '(x*y)_double': lambda df : df.select(df['double_x'] * df['double_y']),
#     '(x/y)_double': lambda df : df.select(df['double_x'] / df['double_y']),
#     '(x+y)_int': lambda df : df.select(df['int_x'] + df['int_y']),
#     '(x-y)_int': lambda df : df.select(df['int_x'] - df['int_y']),
#     '(x*y)_int': lambda df : df.select(df['int_x'] * df['int_y']),
#     '(x/y)_int': lambda df : df.select(df['int_x'] / df['int_y']),
#     'a=a+b_int' : lambda df : loop(lambda df : df.withColumn("int_a", df['int_a'] + df['int_b']), df),
#     'a=a+b_double' : lambda df : loop(lambda df : df.withColumn("double_a", df['double_a'] + df['double_b']), df),
#     'a=a*b_int' : lambda df : loop(lambda df : df.withColumn("int_a", df['int_a'] * df['int_b']), df),
#     'a=a*b_double' : lambda df : loop(lambda df : df.withColumn("double_a", df['double_a'] * df['double_b']), df),
#     'a=a+b*x_int' : lambda df : loop(lambda df : df.withColumn("int_a", df['int_a'] + df['int_b'] * df['int_x']), df),
#     'a=a+b*x_double' : lambda df : loop(lambda df : df.withColumn("double_a", df['double_a'] + df['double_b'] * df['double_x']), df),

# }

# aggregate = {
#     'approx_count_distinct' : lambda df : df.agg(F.approx_count_distinct(df['int_x'])),
#     'avg_double' : lambda df : df.agg(F.avg(df['double_x'])), 
#     'avg(x+y)_double' : lambda df : df.agg(F.avg(df['double_y'] + df['double_x'])), 
#     'avg_int' : lambda df : df.agg(F.avg(df['int_x'])), 
#     'avg(x+y)_int' : lambda df : df.agg(F.avg(df['int_y'] + df['int_x'])), 
#     'corr' : lambda df : df.agg(F.corr(df['double_x'], df['randn'])), 
#     'count' : lambda df : df.agg(F.count(df['value'])), 
#     'countDistinct' : lambda df : df.agg(F.countDistinct(df['value'], df['int_x'])), 
#     'covar_pop' : lambda df : df.agg(F.covar_pop(df['int_x'], df['double_x'])), 
#     'covar_samp' : lambda df : df.agg(F.covar_samp(df['randn'], df['double_x'])), 
#     'kurtosis' : lambda df : df.agg(F.kurtosis(df['randn1'])), 
#     'max' : lambda df : df.agg(F.max(df['double_x'])), 
#     'mean' : lambda df : df.agg(F.mean(df['randn'])), 
#     'min' : lambda df : df.agg(F.min(df['randn'])), 
#     'percentile_approx' : lambda df : df.agg(F.percentile_approx('randn',[0.25,0.5,0.75], 100000)), 
#     'skewness' : lambda df : df.agg(F.skewness(df['randn'])), 
#     'stddev' : lambda df : df.agg(F.stddev(df['randn1'])), 
#     'stddev_pop' : lambda df : df.agg(F.stddev_pop(df['randn1'])), 
#     'stddev_samp' : lambda df : df.agg(F.stddev_samp(df['randn1'])), 
#     'sum_double' : lambda df : df.agg(F.sum(df['double_x'])), 
#     'sum(x+y)_double' : lambda df : df.agg(F.sum(df['double_y'] + df['double_x'])), 
#     'sum_int' : lambda df : df.agg(F.sum(df['int_x'])), 
#     'sum(x+y)_int' : lambda df : df.agg(F.sum(df['int_y'] + df['int_x'])),
#     'sumDistinct' : lambda df : df.agg(F.sumDistinct(df['int_x'])), 
#     'var_pop' : lambda df : df.agg(F.var_pop(df['small_int'])), 
#     'var_samp' : lambda df : df.agg(F.var_samp(df['int_x']))
# }