#!/usr/bin/env python3

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

from timeit import default_timer as timer

def main():
    app_name = f'groups_benchmark'
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    start_time = timer()

    print("Loading data")

    csv = (spark
        .read
        .format("csv")
        .schema(
            T.StructType(
                [
                    T.StructField("_c0", T.DoubleType()), 
                    T.StructField("_c1", T.DoubleType()), 
                    T.StructField("_c2", T.DoubleType())
                ]
            )
        )
        .load("/user/egonzalez/data/XY_doubles_R100000_P100_csv")
    )

    csv.createOrReplaceTempView("test1")
    spark.sql("CACHE TABLE test1").explain(extended=True)

    query = "SELECT _c0, SUM(_c1) as y FROM test1 GROUP BY _c0"
    print("Executing " + query)
    print(spark.sql(query).collect())

if __name__ == '__main__':
    main()
