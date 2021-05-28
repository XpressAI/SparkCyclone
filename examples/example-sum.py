from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import DoubleType, StructType, StructField

def basic_df_example(spark):
    nums = [
        (1.0,),(2.0,),(3.0,),(4.0,),
        (1.0,),(2.0,),(3.0,),(4.0,),
        (1.0,),(2.0,),(3.0,),(4.0,),
        (1.0,),(2.0,),(3.0,),(4.0,),
        (1.0,),(2.0,),(3.0,),(4.0,),
        (1.0,),(2.0,),(3.0,),(4.0,),
        (1.0,),(2.0,),(3.0,),(4.0,),
    ]
    schema = StructType([
        StructField('value', DoubleType(), False),
    ])
    df = spark.createDataFrame(data=nums, schema=schema)
    df.createOrReplaceTempView("nums")
    newDF = spark.sql("SELECT SUM(value) FROM nums")
    newDF.explain()
    newDF.printSchema()
    print(newDF.collect())

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("aurora4spark example") \
        .config("spark.sql.columnVector.offheap.enabled", "true") \
        .getOrCreate()

    basic_df_example(spark)

    spark.stop()
