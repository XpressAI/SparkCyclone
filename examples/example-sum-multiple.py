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
       .load("/opt/aurora4spark/examples/sampleMultiColumn.csv") \
       .selectExpr("SUM(_1)", "SUM(_2)", "SUM(_3)")

    newDF.explain()
    newDF.printSchema()
    print(newDF.collect())

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .config("spark.sql.extensions", "com.nec.spark.LocalVeoExtension") \
        .config("spark.sql.columnVector.offheap.enabled", "true") \
        .getOrCreate()

    basic_df_example(spark)

    spark.stop()