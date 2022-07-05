from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import DoubleType, StructType, StructField

def basic_df_example(spark):
    schema = StructType([
           StructField('_1', DoubleType(), False),
           StructField('_2', DoubleType(), False),
           StructField('_3', DoubleType(), False),
       ])
    newDF = spark \
       .read \
       .format("csv") \
       .schema(schema) \
       .load("file:///opt/cyclone/examples/sampleMultiColumn.csv") \
       .selectExpr("AVG(_2 - _1)", "SUM(2 + _2)", "SUM(_3)")

    newDF.explain()
    newDF.printSchema()
    print(newDF.collect())

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .config("spark.plugins", "io.sparkcyclone.spark.AuroraSqlPlugin") \
        .config("spark.sql.columnVector.offheap.enabled", "true") \
        .getOrCreate()

    basic_df_example(spark)

    spark.stop()