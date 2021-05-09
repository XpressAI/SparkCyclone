from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import DoubleType, StructType, StructField

def basic_df_example(spark):
    nums = [(1.0, 5.0),(2.0, 9.0),(3.0, 10.0),(4.0, 12.0)]
    schema = StructType([
        StructField('_1', DoubleType(), False),
        StructField('_2', DoubleType(), False),
    ])
    df = spark.createDataFrame(data=nums, schema=schema)
    df.createOrReplaceTempView("nums")
    newDF = spark.sql("SELECT SUM(_1 + _2) FROM nums")
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