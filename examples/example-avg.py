from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import DoubleType, StructType, StructField

def basic_df_example(spark):
    nums = [(1.0,),(2.0,),(3.0,),(4.0,)]
    schema = StructType([
        StructField('value', DoubleType(), False),
    ])
    df = spark.createDataFrame(data=nums, schema=schema)
    df.createOrReplaceTempView("nums")
    newDF = spark.sql("SELECT AVG(value) FROM nums")
    newDF.explain()
    newDF.printSchema()
    print(newDF.head())

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("aurora4spark example") \
        .config("spark.sql.extensions", "com.nec.spark.LocalVeoExtension") \
        .getOrCreate()

    basic_df_example(spark)

    spark.stop()