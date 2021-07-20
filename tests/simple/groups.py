#!/usr/bin/env python3

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

from timeit import default_timer as timer

def main():
    app_name = f'groups_benchmark'
    spark = SparkSession.builder.appName(app_name).getOrCreate()

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
        .load("/data/test_file_100m_R100000000_P1000_csv")
    )

    print("Caching test1 table.")
    csv.cache()
    csv.createOrReplaceTempView("test1")
    #spark.sql("CACHE TABLE test1")

    print(spark.sql("SELECT AVG(_c0), AVG(_c1), AVG(_c2) from test1").collect())

    print("Starting 27 Group By SUM queries")
    start = timer()

    for i in range(3):
        for j, col in enumerate(["_c0", "_c1", "_c2"]):
            for k, group in enumerate(["_c0", "_c1", "_c2"]):
                query_start = timer()
                print(spark.sql(f"WITH t(a) AS (SELECT AVG({col}) as a FROM test1 GROUP BY {group}) SELECT AVG(a) FROM t").collect())
                query_end = timer()
                print(f"Finished Iter: {i} Col: {j} Group: {k} Elapsed: {query_end - query_start}s")
    
    end = timer()

    elapsed = end - start
    print(f"Total time for 27 Group By SUM queries: {elapsed}s")


if __name__ == '__main__':
    main()