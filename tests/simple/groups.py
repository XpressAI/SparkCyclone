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
        .load("data/XY_doubles_R100000_P100_csv")
    )

    print("Caching test1 table.")
    csv.createOrReplaceTempView("test1")
    spark.sql("CACHE TABLE test1")

    print(spark.sql("SELECT SUM(_c0), SUM(_c1), SUM(_c2) from test1").collect())

    print("Starting 54 Group By SUM queries:")
    print("Query Plan: ")
    spark.sql(f"WITH t(a) AS (SELECT AVG(_c1) as a FROM test1 GROUP BY _c2) SELECT AVG(a) FROM t").explain(extended=True)

    start = timer()

    for i in range(3):
        for j, col in enumerate(["_c0", "_c1", "_c2"]):
            for k, group in enumerate(["_c0", "_c1", "_c2"]):
                query_start = timer()
                print(spark.sql(f"WITH t(a) AS (SELECT SUM({col}) as a FROM test1 GROUP BY {group}) SELECT SUM(a) FROM t").collect())
                query_end = timer()
                print(f"Finished SUM Iter: {i} Col: {j} Group: {k} Elapsed: {query_end - query_start}s")
                avg_query_start = timer()
                print(spark.sql(f"WITH t(a) AS (SELECT AVG(ABS({col})) as a FROM test1 GROUP BY {group}) SELECT AVG(a) FROM t").collect())
                avg_query_end = timer()
                print(f"Finished AVG Iter: {i} Col: {j} Group: {k} Elapsed: {avg_query_end - avg_query_start}s")

    
    end = timer()

    elapsed = end - start
    print(f"Total time for 54 Group By SUM and AVG queries: {elapsed}s")

    end_time = timer()

    print(f"Total time for entire job: {end_time - start_time}s")

if __name__ == '__main__':
    main()
