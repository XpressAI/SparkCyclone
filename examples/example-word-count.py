from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import DoubleType, StructType, StructField

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .config("spark.sql.extensions", "com.nec.spark.LocalVeoExtension") \
        .config("spark.plugins", "com.nec.spark.AuroraSqlPlugin") \
        .config("spark.sql.codegen.wholeStage", "false") \
        .config("spark.sql.columnVector.offheap.enabled", "true") \
        .getOrCreate()

    try:
        spark \
            .read \
            .text("file:///opt/aurora4spark/examples/sampleOfText.txt") \
            .selectExpr("explode(split(value, ' ')) as word") \
            .createOrReplaceTempView("words")

        df = spark.sql("SELECT word, count(word) AS count FROM words GROUP by word HAVING count > 1 ORDER by count DESC LIMIT 10")
        df.explain()
        print(df.collect())
        
        df = spark.sql("SELECT word, count(word) AS count FROM words GROUP by word ORDER by count DESC LIMIT 10")
        df.explain()
        print(df.collect())
    finally:
        spark.stop()
    