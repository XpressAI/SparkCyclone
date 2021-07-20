import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from typing import Callable

# helper function for looping 
def loop(op: Callable[[DataFrame], DataFrame], df: DataFrame = None) -> DataFrame:
    for _ in range(10):
        df = op(df)
    return df

operations = {
    'abs' : lambda df : df.select(F.abs(df['int_x'])),
    'acos' : lambda df : df.select(F.acos(df['randn'])),
    'acosh' : lambda df : df.select(F.acosh(df['randn'])),
    'ascii': lambda df : df.select(F.ascii(df['prefix2'])),
    'asin': lambda df : df.select(F.asin(df['randn1'])),
    'asinh' : lambda df : df.select(F.asinh(df['randn1'])), 
    'atan' : lambda df : df.select(F.atan(df['randn1'])),
    'atanh' : lambda df : df.select(F.atanh(df['randn1'])),
    'atan2' : lambda df : df.select(F.atan2(df['randn1'], df['randn'])),
    'base64' : lambda df : df.select(F.base64(df['value'])), 
    'bin' : lambda df : df.select(F.bin(df['int_x'])),
    'bitwiseNOT' : lambda df : df.select(F.bitwiseNOT(df['int_x'])), 
    'cbrt' : lambda df : df.select(F.cbrt(df['randn'])),
    'concat' : lambda df : df.select(F.concat(df['prefix2'], df['prefix4'])), 
    'concat_ws' : lambda df : df.select(F.concat_ws('-', df['prefix2'], df['prefix4'], df['float_x'])),
    'conv' : lambda df : df.select(F.conv(df['int_x'], 10, 16)),
    'cos' : lambda df : df.select(F.cos(df['randn'])),
    'cosh' : lambda df : df.select(F.cosh(df['randn'])), 
    'crc32' : lambda df : df.select(F.crc32(df['value'])),
    'degrees' : lambda df : df.select(F.degrees(df['degree'])),
    'exp' : lambda df : df.select(F.exp(df['randn'])), 
    'expr' : lambda df : df.select(F.expr("length(float_x)")),
    'factorial' : lambda df : df.select(F.factorial(df['small_int'])),
    'hash' : lambda df : df.select(F.hash(df['value'])), 
    'hex' : lambda df : df.select(F.hex(df['value'])),
    'hypot' : lambda df : df.select(F.hypot(df['int_x'], df['randn'])),
    'levenshtein' : lambda df : df.select(F.levenshtein(df['value'],df['int_x'])), 
    'log' : lambda df : df.select(F.log(df['int_x'])),
    'log10' : lambda df : df.select(F.log10(df['float_x'])),
    'log1p' : lambda df : df.select(F.log1p(df['randn'])), 
    'log2' : lambda df : df.select(F.log2(df['randn1'])),
    'md5' : lambda df : df.select(F.md5(df['value'])),
    'pow' : lambda df : df.select(F.pow(df['randn'], df['small_int'])),
    'radians' : lambda df : df.select(F.radians(df['degree'])),
    'sha1' : lambda df : df.select(F.sha1(df['value'])),
    'sha2' : lambda df : df.select(F.sha2(df['value'], 256)),
    'signum' : lambda df : df.select(F.signum(df['int_x'])),
    'sin' : lambda df : df.select(F.sin(df['randn'])),
    'sinh' : lambda df : df.select(F.sinh(df['randn'])),
    'sqrt' : lambda df : df.select(F.sqrt(df['small_int'])),
    'tan' : lambda df : df.select(F.tan(df['randn'])),
    'tanh' : lambda df : df.select(F.tanh(df['randn'])),
    'xxhash64' : lambda df : df.select(F.xxhash64(df['value'])),
    'bitwiseAND' : lambda df : df.select(df['small_int'].bitwiseAND(df['int_x'])),
    'bitwiseOR' : lambda df : df.select(df['small_int'].bitwiseOR(df['int_x'])),
    'bitwiseXOR' : lambda df : df.select(df['small_int'].bitwiseXOR(df['int_x'])),
    '(x+y)_float': lambda df : df.select(df['float_x'] + df['float_y']),
    '(x-y)_float': lambda df : df.select(df['float_x'] - df['float_y']),
    '(x*y)_float': lambda df : df.select(df['float_x'] * df['float_y']),
    '(x/y)_float': lambda df : df.select(df['float_x'] / df['float_y']),
    '(x+y)_int': lambda df : df.select(df['int_x'] + df['int_y']),
    '(x-y)_int': lambda df : df.select(df['int_x'] - df['int_y']),
    '(x*y)_int': lambda df : df.select(df['int_x'] * df['int_y']),
    '(x/y)_int': lambda df : df.select(df['int_x'] / df['int_y']),
    'a=a+b_int' : lambda df : loop(lambda df : df.withColumn("int_a", df['int_a'] + df['int_b']), df),
    'a=a+b_float' : lambda df : loop(lambda df : df.withColumn("float_a", df['float_a'] + df['float_b']), df),
    'a=a*b_int' : lambda df : loop(lambda df : df.withColumn("int_a", df['int_a'] * df['int_b']), df),
    'a=a*b_float' : lambda df : loop(lambda df : df.withColumn("float_a", df['float_a'] * df['float_b']), df),
    'a=a+b*x_int' : lambda df : loop(lambda df : df.withColumn("int_a", df['int_a'] + df['int_b'] * df['int_x']), df),
    'a=a+b*x_float' : lambda df : loop(lambda df : df.withColumn("float_a", df['float_a'] + df['float_b'] * df['float_x']), df),

}

aggregate = {
    'approx_count_distinct' : lambda df : df.agg(F.approx_count_distinct(df['int_x'])),
    'avg_float' : lambda df : df.agg(F.avg(df['float_x'])), 
    'avg(x+y)_float' : lambda df : df.agg(F.avg(df['float_y'] + df['float_x'])), 
    'avg_int' : lambda df : df.agg(F.avg(df['int_x'])), 
    'avg(x+y)_int' : lambda df : df.agg(F.avg(df['int_y'] + df['int_x'])), 
    'corr' : lambda df : df.agg(F.corr(df['float_x'], df['randn'])), 
    'count' : lambda df : df.agg(F.count(df['value'])), 
    'countDistinct' : lambda df : df.agg(F.countDistinct(df['value'], df['int_x'])), 
    'covar_pop' : lambda df : df.agg(F.covar_pop(df['int_x'], df['float_x'])), 
    'covar_samp' : lambda df : df.agg(F.covar_samp(df['randn'], df['float_x'])), 
    'kurtosis' : lambda df : df.agg(F.kurtosis(df['randn1'])), 
    'max' : lambda df : df.agg(F.max(df['float_x'])), 
    'mean' : lambda df : df.agg(F.mean(df['randn'])), 
    'min' : lambda df : df.agg(F.min(df['randn'])), 
    'percentile_approx' : lambda df : df.agg(F.percentile_approx('randn',[0.25,0.5,0.75], 100000)), 
    'skewness' : lambda df : df.agg(F.skewness(df['randn'])), 
    'stddev' : lambda df : df.agg(F.stddev(df['randn1'])), 
    'stddev_pop' : lambda df : df.agg(F.stddev_pop(df['randn1'])), 
    'stddev_samp' : lambda df : df.agg(F.stddev_samp(df['randn1'])), 
    'sum_float' : lambda df : df.agg(F.sum(df['float_x'])), 
    'sum(x+y)_float' : lambda df : df.agg(F.sum(df['float_y'] + df['float_x'])), 
    'sum_int' : lambda df : df.agg(F.sum(df['int_x'])), 
    'sum(x+y)_int' : lambda df : df.agg(F.sum(df['int_y'] + df['int_x'])),
    'sumDistinct' : lambda df : df.agg(F.sumDistinct(df['int_x'])), 
    'var_pop' : lambda df : df.agg(F.var_pop(df['small_int'])), 
    'var_samp' : lambda df : df.agg(F.var_samp(df['int_x']))
}

from pyspark.sql import SparkSession, DataFrame

def query_1(spark: SparkSession) -> DataFrame:
    print("Query: SELECT id, SUM(float_x) FROM table GROUP BY id")
    return spark.sql('SELECT id, SUM(float_x) FROM table GROUP BY id')

def query_2(spark: SparkSession) -> DataFrame:
    print("Query: SELECT id, AVG(float_x) FROM table GROUP BY id")
    return spark.sql('SELECT id, AVG(float_x) FROM table GROUP BY id')

def query_3(spark: SparkSession) -> DataFrame:
    print("Query: SELECT id, SUM(float_x + float_y) FROM table GROUP BY id")
    return spark.sql('SELECT id, SUM(float_x + float_y) FROM table GROUP BY id')

def query_4(spark: SparkSession) -> DataFrame:
    print("Query: SELECT id, AVG(float_x + float_y) FROM table GROUP BY id")
    return spark.sql('SELECT id, AVG(float_x + float_y) FROM table GROUP BY id')

def query_5(spark: SparkSession) -> DataFrame:
    print("Query: SELECT id, COUNT(*) FROM table GROUP BY id")
    return spark.sql('SELECT id, COUNT(*) FROM table GROUP BY id')

def query_6(spark: SparkSession) -> DataFrame:
    print("Query: SELECT id, SUM(float_x - float_y) FROM table GROUP BY id")
    return spark.sql('SELECT id, SUM(float_x - float_y) FROM table GROUP BY id')

def query_7(spark: SparkSession) -> DataFrame:
    print("Query: SELECT id, SUM(float_x + float_y) AS res FROM table GROUP BY id HAVING res > 405008")
    return spark.sql("SELECT id, SUM(float_x + float_y) AS res FROM table GROUP BY id HAVING res > 405008")

group_by_queries = {
    'q1': query_1,
    'q2': query_2,
    'q3': query_3,
    'q4': query_4,
    'q5': query_5,
    'q6': query_6,
    'q7': query_7,
}
