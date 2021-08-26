# Progress Checklist for PySpark SQL

## Core Classes

| Functions                                                                                                                                                              | Supported ✅? |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| [SparkSession](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.html#pyspark.sql.SparkSession)(sparkContext\[, jsparkSession\])  |✅|
| [DataFrame](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html#pyspark.sql.DataFrame)(jdf, sql\_ctx)                             |✅|
| [Column](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.html#pyspark.sql.Column)(jc)                                                 |✅|
| [Row](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Row.html#pyspark.sql.Row)                                                              |✅|
| [GroupedData](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.html#pyspark.sql.GroupedData)(jgd, df)                             |⬜️|
| [PandasCogroupedOps](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.PandasCogroupedOps.html#pyspark.sql.PandasCogroupedOps)(gd1, gd2)       |⬜️|
| [DataFrameNaFunctions](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameNaFunctions.html#pyspark.sql.DataFrameNaFunctions)(df)       |⬜️|
| [DataFrameStatFunctions](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameStatFunctions.html#pyspark.sql.DataFrameStatFunctions)(df) |⬜️|
| [Window](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Window.html#pyspark.sql.Window)                                                     |⬜️|

## Spark Session APIs

| Functions                                                                                                                                                                                                            | Supported ✅? |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| [SparkSession.builder.config](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.builder.config.html#pyspark.sql.SparkSession.builder.config)(\[key, value, conf\])              |✅|
| [SparkSession.builder.appName](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.builder.appName.html#pyspark.sql.SparkSession.builder.appName)(name)                           |✅|
| [SparkSession.builder.enableHiveSupport](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.builder.enableHiveSupport.html#pyspark.sql.SparkSession.builder.enableHiveSupport)() |⬜️|
| [SparkSession.builder.getOrCreate](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.builder.getOrCreate.html#pyspark.sql.SparkSession.builder.getOrCreate)()                   |✅|
| [SparkSession.builder.master](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.builder.master.html#pyspark.sql.SparkSession.builder.master)(master)                            |⬜️|
| [SparkSession.catalog](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.catalog.html#pyspark.sql.SparkSession.catalog)                                                         |⬜️|
| [SparkSession.conf](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.conf.html#pyspark.sql.SparkSession.conf)                                                                  |✅|
| [SparkSession.createDataFrame](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.createDataFrame.html#pyspark.sql.SparkSession.createDataFrame)(data\[, schema, …\])            |⬜️|
| [SparkSession.getActiveSession](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.getActiveSession.html#pyspark.sql.SparkSession.getActiveSession)()                            |⬜️|
| [SparkSession.newSession](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.newSession.html#pyspark.sql.SparkSession.newSession)()                                              |⬜️|
| [SparkSession.range](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.range.html#pyspark.sql.SparkSession.range)(start\[, end, step, …\])                                      |⬜️|
| [SparkSession.read](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.read.html#pyspark.sql.SparkSession.read)                                                                  |✅|
| [SparkSession.readStream](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.readStream.html#pyspark.sql.SparkSession.readStream)                                                |⬜️|
| [SparkSession.sparkContext](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.sparkContext.html#pyspark.sql.SparkSession.sparkContext)                                          |✅|
| [SparkSession.sql](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.sql.html#pyspark.sql.SparkSession.sql)(sqlQuery)                                                           |✅|
| [SparkSession.stop](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.stop.html#pyspark.sql.SparkSession.stop)()                                                                |⬜️|
| [SparkSession.streams](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.streams.html#pyspark.sql.SparkSession.streams)                                                         |⬜️|
| [SparkSession.table](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.table.html#pyspark.sql.SparkSession.table)(tableName)                                                    |⬜️|
| [SparkSession.udf](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.udf.html#pyspark.sql.SparkSession.udf)                                                                     |⬜️|
| [SparkSession.version](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.version.html#pyspark.sql.SparkSession.version)                                                         |✅|

## Configuration

| Functions                                                                                                                                                | Supported ✅?                          |
| -------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------|
| [RuntimeConfig](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.conf.RuntimeConfig.html#pyspark.sql.conf.RuntimeConfig)(jconf) |⬜️|

## Input and Output

| Functions                                                                                                                                                                                              | Supported ✅?                            |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------- |
| [DataFrameReader.csv](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.csv.html#pyspark.sql.DataFrameReader.csv)(path\[, schema, sep, …\])                    | ✅ |
| [DataFrameReader.format](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.format.html#pyspark.sql.DataFrameReader.format)(source)                             | ✅ |
| [DataFrameReader.jdbc](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.jdbc.html#pyspark.sql.DataFrameReader.jdbc)(url, table\[, column, …\])                | ⬜️ |
| [DataFrameReader.json](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.json.html#pyspark.sql.DataFrameReader.json)(path\[, schema, …\])                      | ⬜️ |
| [DataFrameReader.load](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.load.html#pyspark.sql.DataFrameReader.load)(\[path, format, schema\])                 | ⬜️ |
| [DataFrameReader.option](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.option.html#pyspark.sql.DataFrameReader.option)(key, value)                         | ⬜️ |
| [DataFrameReader.options](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.options.html#pyspark.sql.DataFrameReader.options)(\*\*options)                     | ⬜️ |
| [DataFrameReader.orc](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.orc.html#pyspark.sql.DataFrameReader.orc)(path\[, mergeSchema, …\])                    | ⬜️ |
| [DataFrameReader.parquet](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.parquet.html#pyspark.sql.DataFrameReader.parquet)(\*paths, \*\*options)            | ✅ |
| [DataFrameReader.schema](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.schema.html#pyspark.sql.DataFrameReader.schema)(schema)                             | ✅ |
| [DataFrameReader.table](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.table.html#pyspark.sql.DataFrameReader.table)(tableName)                             | ⬜️ |
| [DataFrameWriter.bucketBy](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameWriter.bucketBy.html#pyspark.sql.DataFrameWriter.bucketBy)(numBuckets, col, \*cols)      | ⬜️ |
| [DataFrameWriter.csv](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameWriter.csv.html#pyspark.sql.DataFrameWriter.csv)(path\[, mode, …\])                           | ✅ |
| [DataFrameWriter.format](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameWriter.format.html#pyspark.sql.DataFrameWriter.format)(source)                             | ⬜️ |
| [DataFrameWriter.insertInto](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameWriter.insertInto.html#pyspark.sql.DataFrameWriter.insertInto)(tableName\[, …\])       | ⬜️ |
| [DataFrameWriter.jdbc](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameWriter.jdbc.html#pyspark.sql.DataFrameWriter.jdbc)(url, table\[, mode, …\])                  | ⬜️ |
| [DataFrameWriter.json](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameWriter.json.html#pyspark.sql.DataFrameWriter.json)(path\[, mode, …\])                        | ⬜️ |
| [DataFrameWriter.mode](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameWriter.mode.html#pyspark.sql.DataFrameWriter.mode)(saveMode)                                 | ⬜️ |
| [DataFrameWriter.option](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameWriter.option.html#pyspark.sql.DataFrameWriter.option)(key, value)                         | ⬜️ |
| [DataFrameWriter.options](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameWriter.options.html#pyspark.sql.DataFrameWriter.options)(\*\*options)                     | ⬜️ |
| [DataFrameWriter.orc](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameWriter.orc.html#pyspark.sql.DataFrameWriter.orc)(path\[, mode, …\])                           | ⬜️ |
| [DataFrameWriter.parquet](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameWriter.parquet.html#pyspark.sql.DataFrameWriter.parquet)(path\[, mode, …\])               | ✅ |
| [DataFrameWriter.partitionBy](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameWriter.partitionBy.html#pyspark.sql.DataFrameWriter.partitionBy)(\*cols)              | ⬜️ |
| [DataFrameWriter.save](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameWriter.save.html#pyspark.sql.DataFrameWriter.save)(\[path, format, mode, …\])                | ✅ |
| [DataFrameWriter.saveAsTable](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameWriter.saveAsTable.html#pyspark.sql.DataFrameWriter.saveAsTable)(name\[, format, …\]) | ⬜️ |
| [DataFrameWriter.sortBy](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameWriter.sortBy.html#pyspark.sql.DataFrameWriter.sortBy)(col, \*cols)                        | ✅ |
| [DataFrameWriter.text](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameWriter.text.html#pyspark.sql.DataFrameWriter.text)(path\[, compression, …\])                 | ⬜️ |

## DataFrame APIs

| Functions                                                    | Supported ✅? |
| ------------------------------------------------------------ | ------------ |
| [DataFrame.agg](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.agg.html#pyspark.sql.DataFrame.agg)(\*exprs) | ✅            |
| [DataFrame.alias](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.alias.html#pyspark.sql.DataFrame.alias)(alias) | ⬜️            |
| [DataFrame.approxQuantile](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.approxQuantile.html#pyspark.sql.DataFrame.approxQuantile)(col, probabilities, …) | ⬜️            |
| [DataFrame.cache](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.cache.html#pyspark.sql.DataFrame.cache)() | ✅            |
| [DataFrame.checkpoint](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.checkpoint.html#pyspark.sql.DataFrame.checkpoint)(\[eager\]) | ⬜️            |
| [DataFrame.coalesce](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.coalesce.html#pyspark.sql.DataFrame.coalesce)(numPartitions) | ✅            |
| [DataFrame.colRegex](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.colRegex.html#pyspark.sql.DataFrame.colRegex)(colName) | ⬜️            |
| [DataFrame.collect](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.collect.html#pyspark.sql.DataFrame.collect)() | ✅            |
| [DataFrame.columns](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.columns.html#pyspark.sql.DataFrame.columns) | ✅            |
| [DataFrame.corr](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.corr.html#pyspark.sql.DataFrame.corr)(col1, col2\[, method\]) | ⬜️            |
| [DataFrame.count](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.count.html#pyspark.sql.DataFrame.count)() | ✅            |
| [DataFrame.cov](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.cov.html#pyspark.sql.DataFrame.cov)(col1, col2) | ⬜️            |
| [DataFrame.createGlobalTempView](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.createGlobalTempView.html#pyspark.sql.DataFrame.createGlobalTempView)(name) | ⬜️            |
| [DataFrame.createOrReplaceGlobalTempView](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.createOrReplaceGlobalTempView.html#pyspark.sql.DataFrame.createOrReplaceGlobalTempView)(name) | ✅            |
| [DataFrame.createOrReplaceTempView](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.createOrReplaceTempView.html#pyspark.sql.DataFrame.createOrReplaceTempView)(name) | ✅            |
| [DataFrame.createTempView](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.createTempView.html#pyspark.sql.DataFrame.createTempView)(name) | ✅            |
| [DataFrame.crossJoin](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.crossJoin.html#pyspark.sql.DataFrame.crossJoin)(other) | ⬜️            |
| [DataFrame.crosstab](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.crosstab.html#pyspark.sql.DataFrame.crosstab)(col1, col2) | ⬜️            |
| [DataFrame.cube](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.cube.html#pyspark.sql.DataFrame.cube)(\*cols) | ⬜️            |
| [DataFrame.describe](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.describe.html#pyspark.sql.DataFrame.describe)(\*cols) | ⬜️            |
| [DataFrame.distinct](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.distinct.html#pyspark.sql.DataFrame.distinct)() | ⬜️            |
| [DataFrame.drop](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.drop.html#pyspark.sql.DataFrame.drop)(\*cols) | ⬜️            |
| [DataFrame.dropDuplicates](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.dropDuplicates.html#pyspark.sql.DataFrame.dropDuplicates)(\[subset\]) | ⬜️            |
| [DataFrame.drop\_duplicates](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.drop_duplicates.html#pyspark.sql.DataFrame.drop_duplicates)(\[subset\]) | ⬜️            |
| [DataFrame.dropna](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.dropna.html#pyspark.sql.DataFrame.dropna)(\[how, thresh, subset\]) | ⬜️            |
| [DataFrame.dtypes](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.dtypes.html#pyspark.sql.DataFrame.dtypes) | ⬜️            |
| [DataFrame.exceptAll](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.exceptAll.html#pyspark.sql.DataFrame.exceptAll)(other) | ⬜️            |
| [DataFrame.explain](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.explain.html#pyspark.sql.DataFrame.explain)(\[extended, mode\]) | ⬜️            |
| [DataFrame.fillna](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.fillna.html#pyspark.sql.DataFrame.fillna)(value\[, subset\]) | ⬜️            |
| [DataFrame.filter](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.filter.html#pyspark.sql.DataFrame.filter)(condition) | ⬜️            |
| [DataFrame.first](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.first.html#pyspark.sql.DataFrame.first)() | ⬜️            |
| [DataFrame.foreach](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.foreach.html#pyspark.sql.DataFrame.foreach)(f) | ⬜️            |
| [DataFrame.foreachPartition](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.foreachPartition.html#pyspark.sql.DataFrame.foreachPartition)(f) | ⬜️            |
| [DataFrame.freqItems](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.freqItems.html#pyspark.sql.DataFrame.freqItems)(cols\[, support\]) | ⬜️            |
| [DataFrame.groupBy](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.groupBy.html#pyspark.sql.DataFrame.groupBy)(\*cols) | ✅            |
| [DataFrame.head](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.head.html#pyspark.sql.DataFrame.head)(\[n\]) | ⬜️            |
| [DataFrame.hint](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.hint.html#pyspark.sql.DataFrame.hint)(name, \*parameters) | ⬜️            |
| [DataFrame.inputFiles](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.inputFiles.html#pyspark.sql.DataFrame.inputFiles)() | ⬜️            |
| [DataFrame.intersect](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.intersect.html#pyspark.sql.DataFrame.intersect)(other) | ⬜️            |
| [DataFrame.intersectAll](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.intersectAll.html#pyspark.sql.DataFrame.intersectAll)(other) | ⬜️            |
| [DataFrame.isLocal](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.isLocal.html#pyspark.sql.DataFrame.isLocal)() | ⬜️            |
| [DataFrame.isStreaming](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.isStreaming.html#pyspark.sql.DataFrame.isStreaming) | ⬜️            |
| [DataFrame.join](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.join.html#pyspark.sql.DataFrame.join)(other\[, on, how\]) | ⬜️            |
| [DataFrame.limit](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.limit.html#pyspark.sql.DataFrame.limit)(num) | ⬜️            |
| [DataFrame.localCheckpoint](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.localCheckpoint.html#pyspark.sql.DataFrame.localCheckpoint)(\[eager\]) | ⬜️            |
| [DataFrame.mapInPandas](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.mapInPandas.html#pyspark.sql.DataFrame.mapInPandas)(func, schema) | ⬜️            |
| [DataFrame.na](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.na.html#pyspark.sql.DataFrame.na) | ⬜️            |
| [DataFrame.orderBy](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.orderBy.html#pyspark.sql.DataFrame.orderBy)(\*cols, \*\*kwargs) | ✅            |
| [DataFrame.persist](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.persist.html#pyspark.sql.DataFrame.persist)(\[storageLevel\]) | ✅            |
| [DataFrame.printSchema](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.printSchema.html#pyspark.sql.DataFrame.printSchema)() | ✅            |
| [DataFrame.randomSplit](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.randomSplit.html#pyspark.sql.DataFrame.randomSplit)(weights\[, seed\]) | ⬜️            |
| [DataFrame.rdd](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.rdd.html#pyspark.sql.DataFrame.rdd) | ⬜️            |
| [DataFrame.registerTempTable](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.registerTempTable.html#pyspark.sql.DataFrame.registerTempTable)(name) | ✅            |
| [DataFrame.repartition](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.repartition.html#pyspark.sql.DataFrame.repartition)(numPartitions, \*cols) | ⬜️            |
| [DataFrame.repartitionByRange](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.repartitionByRange.html#pyspark.sql.DataFrame.repartitionByRange)(numPartitions, …) | ⬜️            |
| [DataFrame.replace](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.replace.html#pyspark.sql.DataFrame.replace)(to\_replace\[, value, subset\]) | ⬜️            |
| [DataFrame.rollup](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.rollup.html#pyspark.sql.DataFrame.rollup)(\*cols) | ⬜️            |
| [DataFrame.sameSemantics](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.sameSemantics.html#pyspark.sql.DataFrame.sameSemantics)(other) | ⬜️            |
| [DataFrame.sample](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.sample.html#pyspark.sql.DataFrame.sample)(\[withReplacement, …\]) | ⬜️            |
| [DataFrame.sampleBy](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.sampleBy.html#pyspark.sql.DataFrame.sampleBy)(col, fractions\[, seed\]) | ⬜️            |
| [DataFrame.schema](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.schema.html#pyspark.sql.DataFrame.schema) | ✅            |
| [DataFrame.select](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.select.html#pyspark.sql.DataFrame.select)(\*cols) | ✅            |
| [DataFrame.selectExpr](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.selectExpr.html#pyspark.sql.DataFrame.selectExpr)(\*expr) | ⬜️            |
| [DataFrame.semanticHash](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.semanticHash.html#pyspark.sql.DataFrame.semanticHash)() | ⬜️            |
| [DataFrame.show](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.show.html#pyspark.sql.DataFrame.show)(\[n, truncate, vertical\]) | ⬜️            |
| [DataFrame.sort](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.sort.html#pyspark.sql.DataFrame.sort)(\*cols, \*\*kwargs) | ✅            |
| [DataFrame.sortWithinPartitions](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.sortWithinPartitions.html#pyspark.sql.DataFrame.sortWithinPartitions)(\*cols, \*\*kwargs) | ⬜️            |
| [DataFrame.stat](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.stat.html#pyspark.sql.DataFrame.stat) | ⬜️            |
| [DataFrame.storageLevel](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.storageLevel.html#pyspark.sql.DataFrame.storageLevel) | ✅            |
| [DataFrame.subtract](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.subtract.html#pyspark.sql.DataFrame.subtract)(other) | ⬜️            |
| [DataFrame.summary](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.summary.html#pyspark.sql.DataFrame.summary)(\*statistics) | ⬜️            |
| [DataFrame.tail](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.tail.html#pyspark.sql.DataFrame.tail)(num) | ⬜️            |
| [DataFrame.take](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.take.html#pyspark.sql.DataFrame.take)(num) | ⬜️            |
| [DataFrame.toDF](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.toDF.html#pyspark.sql.DataFrame.toDF)(\*cols) | ⬜️            |
| [DataFrame.toJSON](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.toJSON.html#pyspark.sql.DataFrame.toJSON)(\[use\_unicode\]) | ⬜️            |
| [DataFrame.toLocalIterator](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.toLocalIterator.html#pyspark.sql.DataFrame.toLocalIterator)(\[prefetchPartitions\]) | ⬜️            |
| [DataFrame.toPandas](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.toPandas.html#pyspark.sql.DataFrame.toPandas)() | ⬜️            |
| [DataFrame.transform](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.transform.html#pyspark.sql.DataFrame.transform)(func) | ⬜️            |
| [DataFrame.union](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.union.html#pyspark.sql.DataFrame.union)(other) | ⬜️            |
| [DataFrame.unionAll](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.unionAll.html#pyspark.sql.DataFrame.unionAll)(other) | ⬜️            |
| [DataFrame.unionByName](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.unionByName.html#pyspark.sql.DataFrame.unionByName)(other\[, …\]) | ⬜️            |
| [DataFrame.unpersist](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.unpersist.html#pyspark.sql.DataFrame.unpersist)(\[blocking\]) | ✅            |
| [DataFrame.where](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.where.html#pyspark.sql.DataFrame.where)(condition) | ✅            |
| [DataFrame.withColumn](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html#pyspark.sql.DataFrame.withColumn)(colName, col) | ⬜️            |
| [DataFrame.withColumnRenamed](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.withColumnRenamed.html#pyspark.sql.DataFrame.withColumnRenamed)(existing, new) | ⬜️            |
| [DataFrame.withWatermark](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.withWatermark.html#pyspark.sql.DataFrame.withWatermark)(eventTime, …) | ⬜️            |
| [DataFrame.write](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.write.html#pyspark.sql.DataFrame.write) | ✅            |
| [DataFrame.writeStream](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.writeStream.html#pyspark.sql.DataFrame.writeStream) | ⬜️            |
| [DataFrame.writeTo](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.writeTo.html#pyspark.sql.DataFrame.writeTo)(table) | ⬜️            |
| [DataFrameNaFunctions.drop](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameNaFunctions.drop.html#pyspark.sql.DataFrameNaFunctions.drop)(\[how, thresh, subset\]) | ⬜️            |
| [DataFrameNaFunctions.fill](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameNaFunctions.fill.html#pyspark.sql.DataFrameNaFunctions.fill)(value\[, subset\]) | ⬜️            |
| [DataFrameNaFunctions.replace](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameNaFunctions.replace.html#pyspark.sql.DataFrameNaFunctions.replace)(to\_replace\[, …\]) | ⬜️            |
| [DataFrameStatFunctions.approxQuantile](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameStatFunctions.approxQuantile.html#pyspark.sql.DataFrameStatFunctions.approxQuantile)(col, …) | ⬜️            |
| [DataFrameStatFunctions.corr](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameStatFunctions.corr.html#pyspark.sql.DataFrameStatFunctions.corr)(col1, col2\[, method\]) | ✅            |
| [DataFrameStatFunctions.cov](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameStatFunctions.cov.html#pyspark.sql.DataFrameStatFunctions.cov)(col1, col2) | ⬜️            |
| [DataFrameStatFunctions.crosstab](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameStatFunctions.crosstab.html#pyspark.sql.DataFrameStatFunctions.crosstab)(col1, col2) | ⬜️            |
| [DataFrameStatFunctions.freqItems](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameStatFunctions.freqItems.html#pyspark.sql.DataFrameStatFunctions.freqItems)(cols\[, support\]) | ⬜️            |
| [DataFrameStatFunctions.sampleBy](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameStatFunctions.sampleBy.html#pyspark.sql.DataFrameStatFunctions.sampleBy)(col, fractions) | ⬜️            |

## Column APIs

| Functions                                                                                                                                                                 | Supported ✅?                            |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- |
| [Column.alias](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.alias.html#pyspark.sql.Column.alias)(\*alias, \*\*kwargs)                 | ⬜️ |
| [Column.asc](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.asc.html#pyspark.sql.Column.asc)()                                          | ⬜️ |
| [Column.asc\_nulls\_first](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.asc_nulls_first.html#pyspark.sql.Column.asc_nulls_first)()    | ⬜️ |
| [Column.asc\_nulls\_last](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.asc_nulls_last.html#pyspark.sql.Column.asc_nulls_last)()       | ⬜️ |
| [Column.astype](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.astype.html#pyspark.sql.Column.astype)(dataType)                         | ⬜️ |
| [Column.between](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.between.html#pyspark.sql.Column.between)(lowerBound, upperBound)        | ⬜️ |
| [Column.bitwiseAND](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.bitwiseAND.html#pyspark.sql.Column.bitwiseAND)(other)                | ⬜️ |
| [Column.bitwiseOR](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.bitwiseOR.html#pyspark.sql.Column.bitwiseOR)(other)                   | ⬜️ |
| [Column.bitwiseXOR](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.bitwiseXOR.html#pyspark.sql.Column.bitwiseXOR)(other)                | ⬜️ |
| [Column.cast](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.cast.html#pyspark.sql.Column.cast)(dataType)                               | ⬜️ |
| [Column.contains](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.contains.html#pyspark.sql.Column.contains)(other)                      | ⬜️ |
| [Column.desc](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.desc.html#pyspark.sql.Column.desc)()                                       | ⬜️ |
| [Column.desc\_nulls\_first](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.desc_nulls_first.html#pyspark.sql.Column.desc_nulls_first)() | ⬜️ |
| [Column.desc\_nulls\_last](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.desc_nulls_last.html#pyspark.sql.Column.desc_nulls_last)()    | ⬜️ |
| [Column.dropFields](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.dropFields.html#pyspark.sql.Column.dropFields)(\*fieldNames)         | ⬜️ |
| [Column.endswith](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.endswith.html#pyspark.sql.Column.endswith)(other)                      | ⬜️ |
| [Column.eqNullSafe](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.eqNullSafe.html#pyspark.sql.Column.eqNullSafe)(other)                | ⬜️ |
| [Column.getField](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.getField.html#pyspark.sql.Column.getField)(name)                       | ⬜️ |
| [Column.getItem](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.getItem.html#pyspark.sql.Column.getItem)(key)                           | ⬜️ |
| [Column.isNotNull](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.isNotNull.html#pyspark.sql.Column.isNotNull)()                        | ⬜️ |
| [Column.isNull](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.isNull.html#pyspark.sql.Column.isNull)()                                 | ⬜️ |
| [Column.isin](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.isin.html#pyspark.sql.Column.isin)(\*cols)                                 | ⬜️ |
| [Column.like](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.like.html#pyspark.sql.Column.like)(other)                                  | ⬜️ |
| [Column.name](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.name.html#pyspark.sql.Column.name)(\*alias, \*\*kwargs)                    | ⬜️ |
| [Column.otherwise](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.otherwise.html#pyspark.sql.Column.otherwise)(value)                   | ⬜️ |
| [Column.over](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.over.html#pyspark.sql.Column.over)(window)                                 | ⬜️ |
| [Column.rlike](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.rlike.html#pyspark.sql.Column.rlike)(other)                               | ⬜️ |
| [Column.startswith](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.startswith.html#pyspark.sql.Column.startswith)(other)                | ⬜️ |
| [Column.substr](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.substr.html#pyspark.sql.Column.substr)(startPos, length)                 | ✅ |
| [Column.when](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.when.html#pyspark.sql.Column.when)(condition, value)                       | ⬜️ |
| [Column.withField](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.withField.html#pyspark.sql.Column.withField)(fieldName, col)          | ⬜️ |

## Data Types

| Functions                                                                                                                                                                             | Supported ✅?                            |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- |
| [ArrayType](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.ArrayType.html#pyspark.sql.types.ArrayType)(elementType\[, containsNull\])                | ⬜️ |
| [BinaryType](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.BinaryType.html#pyspark.sql.types.BinaryType)                                            | ⬜️ |
| [BooleanType](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.BooleanType.html#pyspark.sql.types.BooleanType)                                         | ⬜️ |
| [ByteType](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.ByteType.html#pyspark.sql.types.ByteType)                                                  | ⬜️ |
| [DataType](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.DataType.html#pyspark.sql.types.DataType)                                                  | ⬜️ |
| [DateType](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.DateType.html#pyspark.sql.types.DateType)                                                  | ⬜️ |
| [DecimalType](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.DecimalType.html#pyspark.sql.types.DecimalType)(\[precision, scale\])                   | ⬜️ |
| [DoubleType](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.DoubleType.html#pyspark.sql.types.DoubleType)                                            | ✅ |
| [FloatType](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.FloatType.html#pyspark.sql.types.FloatType)                                               | ✅ |
| [IntegerType](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.IntegerType.html#pyspark.sql.types.IntegerType)                                         | ✅ |
| [LongType](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.LongType.html#pyspark.sql.types.LongType)                                                  | ✅ |
| [MapType](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.MapType.html#pyspark.sql.types.MapType)(keyType, valueType\[, valueContainsNull\])          | ⬜️ |
| [NullType](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.NullType.html#pyspark.sql.types.NullType)                                                  | ⬜️ |
| [ShortType](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.ShortType.html#pyspark.sql.types.ShortType)                                               | ⬜️ |
| [StringType](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.StringType.html#pyspark.sql.types.StringType)                                            | ⬜️ |
| [StructField](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.StructField.html#pyspark.sql.types.StructField)(name, dataType\[, nullable, metadata\]) | ⬜️ |
| [StructType](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.StructType.html#pyspark.sql.types.StructType)(\[fields\])                                | ⬜️ |
| [TimestampType](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.TimestampType.html#pyspark.sql.types.TimestampType)                                   | ⬜️ |

## Row

| Functions                                                                                                                                     | Supported ✅?                            |
| --------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- |
| [Row.asDict](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Row.asDict.html#pyspark.sql.Row.asDict)(\[recursive\]) | ⬜️ |

## Functions

| Functions                                                    | Supported ✅? |
| ------------------------------------------------------------ | ------------ |
| [abs](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.abs.html#pyspark.sql.functions.abs)(col) | ⬜️            |
| [acos](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.acos.html#pyspark.sql.functions.acos)(col) | ⬜️            |
| [acosh](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.acosh.html#pyspark.sql.functions.acosh)(col) | ⬜️            |
| [add\_months](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.add_months.html#pyspark.sql.functions.add_months)(start, months) | ⬜️            |
| [aggregate](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.aggregate.html#pyspark.sql.functions.aggregate)(col, initialValue, merge\[, finish\]) | ✅            |
| [approxCountDistinct](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.approxCountDistinct.html#pyspark.sql.functions.approxCountDistinct)(col\[, rsd\]) | ⬜️            |
| [approx\_count\_distinct](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.approx_count_distinct.html#pyspark.sql.functions.approx_count_distinct)(col\[, rsd\]) | ⬜️            |
| [array](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.array.html#pyspark.sql.functions.array)(\*cols) | ⬜️            |
| [array\_contains](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.array_contains.html#pyspark.sql.functions.array_contains)(col, value) | ⬜️            |
| [array\_distinct](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.array_distinct.html#pyspark.sql.functions.array_distinct)(col) | ⬜️            |
| [array\_except](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.array_except.html#pyspark.sql.functions.array_except)(col1, col2) | ⬜️            |
| [array\_intersect](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.array_intersect.html#pyspark.sql.functions.array_intersect)(col1, col2) | ⬜️            |
| [array\_join](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.array_join.html#pyspark.sql.functions.array_join)(col, delimiter\[, null\_replacement\]) | ⬜️            |
| [array\_max](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.array_max.html#pyspark.sql.functions.array_max)(col) | ⬜️            |
| [array\_min](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.array_min.html#pyspark.sql.functions.array_min)(col) | ⬜️            |
| [array\_position](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.array_position.html#pyspark.sql.functions.array_position)(col, value) | ⬜️            |
| [array\_remove](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.array_remove.html#pyspark.sql.functions.array_remove)(col, element) | ⬜️            |
| [array\_repeat](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.array_repeat.html#pyspark.sql.functions.array_repeat)(col, count) | ⬜️            |
| [array\_sort](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.array_sort.html#pyspark.sql.functions.array_sort)(col) | ⬜️            |
| [array\_union](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.array_union.html#pyspark.sql.functions.array_union)(col1, col2) | ⬜️            |
| [arrays\_overlap](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.arrays_overlap.html#pyspark.sql.functions.arrays_overlap)(a1, a2) | ⬜️            |
| [arrays\_zip](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.arrays_zip.html#pyspark.sql.functions.arrays_zip)(\*cols) | ⬜️            |
| [asc](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.asc.html#pyspark.sql.functions.asc)(col) | ⬜️            |
| [asc\_nulls\_first](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.asc_nulls_first.html#pyspark.sql.functions.asc_nulls_first)(col) | ⬜️            |
| [asc\_nulls\_last](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.asc_nulls_last.html#pyspark.sql.functions.asc_nulls_last)(col) | ⬜️            |
| [ascii](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.ascii.html#pyspark.sql.functions.ascii)(col) | ⬜️            |
| [asin](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.asin.html#pyspark.sql.functions.asin)(col) | ⬜️            |
| [asinh](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.asinh.html#pyspark.sql.functions.asinh)(col) | ⬜️            |
| [assert\_true](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.assert_true.html#pyspark.sql.functions.assert_true)(col\[, errMsg\]) | ⬜️            |
| [atan](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.atan.html#pyspark.sql.functions.atan)(col) | ⬜️            |
| [atanh](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.atanh.html#pyspark.sql.functions.atanh)(col) | ⬜️            |
| [atan2](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.atan2.html#pyspark.sql.functions.atan2)(col1, col2) | ⬜️            |
| [avg](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.avg.html#pyspark.sql.functions.avg)(col) | ✅            |
| [base64](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.base64.html#pyspark.sql.functions.base64)(col) | ⬜️            |
| [bin](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.bin.html#pyspark.sql.functions.bin)(col) | ⬜️            |
| [bitwiseNOT](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.bitwiseNOT.html#pyspark.sql.functions.bitwiseNOT)(col) | ⬜️            |
| [broadcast](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.broadcast.html#pyspark.sql.functions.broadcast)(df) | ⬜️            |
| [bround](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.bround.html#pyspark.sql.functions.bround)(col\[, scale\]) | ⬜️            |
| [bucket](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.bucket.html#pyspark.sql.functions.bucket)(numBuckets, col) | ⬜️            |
| [cbrt](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.cbrt.html#pyspark.sql.functions.cbrt)(col) | ⬜️            |
| [ceil](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.ceil.html#pyspark.sql.functions.ceil)(col) | ⬜️            |
| [coalesce](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.coalesce.html#pyspark.sql.functions.coalesce)(\*cols) | ⬜️            |
| [col](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.col.html#pyspark.sql.functions.col)(col) | ⬜️            |
| [collect\_list](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.collect_list.html#pyspark.sql.functions.collect_list)(col) | ⬜️            |
| [collect\_set](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.collect_set.html#pyspark.sql.functions.collect_set)(col) | ⬜️            |
| [column](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.column.html#pyspark.sql.functions.column)(col) | ✅            |
| [concat](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.concat.html#pyspark.sql.functions.concat)(\*cols) | ⬜️            |
| [concat\_ws](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.concat_ws.html#pyspark.sql.functions.concat_ws)(sep, \*cols) | ⬜️            |
| [conv](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.conv.html#pyspark.sql.functions.conv)(col, fromBase, toBase) | ⬜️            |
| [corr](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.corr.html#pyspark.sql.functions.corr)(col1, col2) | ✅            |
| [cos](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.cos.html#pyspark.sql.functions.cos)(col) | ⬜️            |
| [cosh](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.cosh.html#pyspark.sql.functions.cosh)(col) | ⬜️            |
| [count](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.count.html#pyspark.sql.functions.count)(col) | ✅            |
| [countDistinct](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.countDistinct.html#pyspark.sql.functions.countDistinct)(col, \*cols) | ⬜️            |
| [covar\_pop](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.covar_pop.html#pyspark.sql.functions.covar_pop)(col1, col2) | ⬜️            |
| [covar\_samp](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.covar_samp.html#pyspark.sql.functions.covar_samp)(col1, col2) | ⬜️            |
| [crc32](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.crc32.html#pyspark.sql.functions.crc32)(col) | ⬜️            |
| [create\_map](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.create_map.html#pyspark.sql.functions.create_map)(\*cols) | ⬜️            |
| [cume\_dist](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.cume_dist.html#pyspark.sql.functions.cume_dist)() | ⬜️            |
| [current\_date](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.current_date.html#pyspark.sql.functions.current_date)() | ⬜️            |
| [current\_timestamp](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.current_timestamp.html#pyspark.sql.functions.current_timestamp)() | ⬜️            |
| [date\_add](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.date_add.html#pyspark.sql.functions.date_add)(start, days) | ⬜️            |
| [date\_format](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.date_format.html#pyspark.sql.functions.date_format)(date, format) | ⬜️            |
| [date\_sub](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.date_sub.html#pyspark.sql.functions.date_sub)(start, days) | ⬜️            |
| [date\_trunc](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.date_trunc.html#pyspark.sql.functions.date_trunc)(format, timestamp) | ⬜️            |
| [datediff](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.datediff.html#pyspark.sql.functions.datediff)(end, start) | ⬜️            |
| [dayofmonth](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.dayofmonth.html#pyspark.sql.functions.dayofmonth)(col) | ⬜️            |
| [dayofweek](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.dayofweek.html#pyspark.sql.functions.dayofweek)(col) | ⬜️            |
| [dayofyear](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.dayofyear.html#pyspark.sql.functions.dayofyear)(col) | ⬜️            |
| [days](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.days.html#pyspark.sql.functions.days)(col) | ⬜️            |
| [decode](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.decode.html#pyspark.sql.functions.decode)(col, charset) | ⬜️            |
| [degrees](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.degrees.html#pyspark.sql.functions.degrees)(col) | ⬜️            |
| [dense\_rank](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.dense_rank.html#pyspark.sql.functions.dense_rank)() | ⬜️            |
| [desc](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.desc.html#pyspark.sql.functions.desc)(col) | ⬜️            |
| [desc\_nulls\_first](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.desc_nulls_first.html#pyspark.sql.functions.desc_nulls_first)(col) | ⬜️            |
| [desc\_nulls\_last](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.desc_nulls_last.html#pyspark.sql.functions.desc_nulls_last)(col) | ⬜️            |
| [element\_at](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.element_at.html#pyspark.sql.functions.element_at)(col, extraction) | ⬜️            |
| [encode](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.encode.html#pyspark.sql.functions.encode)(col, charset) | ⬜️            |
| [exists](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.exists.html#pyspark.sql.functions.exists)(col, f) | ⬜️            |
| [exp](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.exp.html#pyspark.sql.functions.exp)(col) | ⬜️            |
| [explode](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.explode.html#pyspark.sql.functions.explode)(col) | ⬜️            |
| [explode\_outer](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.explode_outer.html#pyspark.sql.functions.explode_outer)(col) | ⬜️            |
| [expm1](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.expm1.html#pyspark.sql.functions.expm1)(col) | ⬜️            |
| [expr](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.expr.html#pyspark.sql.functions.expr)(str) | ✅            |
| [factorial](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.factorial.html#pyspark.sql.functions.factorial)(col) | ⬜️            |
| [filter](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.filter.html#pyspark.sql.functions.filter)(col, f) | ⬜️            |
| [first](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.first.html#pyspark.sql.functions.first)(col\[, ignorenulls\]) | ⬜️            |
| [flatten](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.flatten.html#pyspark.sql.functions.flatten)(col) | ⬜️            |
| [floor](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.floor.html#pyspark.sql.functions.floor)(col) | ⬜️            |
| [forall](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.forall.html#pyspark.sql.functions.forall)(col, f) | ⬜️            |
| [format\_number](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.format_number.html#pyspark.sql.functions.format_number)(col, d) | ⬜️            |
| [format\_string](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.format_string.html#pyspark.sql.functions.format_string)(format, \*cols) | ⬜️            |
| [from\_csv](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.from_csv.html#pyspark.sql.functions.from_csv)(col, schema\[, options\]) | ⬜️            |
| [from\_json](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.from_json.html#pyspark.sql.functions.from_json)(col, schema\[, options\]) | ⬜️            |
| [from\_unixtime](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.from_unixtime.html#pyspark.sql.functions.from_unixtime)(timestamp\[, format\]) | ⬜️            |
| [from\_utc\_timestamp](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.from_utc_timestamp.html#pyspark.sql.functions.from_utc_timestamp)(timestamp, tz) | ⬜️            |
| [get\_json\_object](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.get_json_object.html#pyspark.sql.functions.get_json_object)(col, path) | ⬜️            |
| [greatest](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.greatest.html#pyspark.sql.functions.greatest)(\*cols) | ⬜️            |
| [grouping](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.grouping.html#pyspark.sql.functions.grouping)(col) | ⬜️            |
| [grouping\_id](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.grouping_id.html#pyspark.sql.functions.grouping_id)(\*cols) | ⬜️            |
| [hash](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.hash.html#pyspark.sql.functions.hash)(\*cols) | ⬜️            |
| [hex](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.hex.html#pyspark.sql.functions.hex)(col) | ⬜️            |
| [hour](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.hour.html#pyspark.sql.functions.hour)(col) | ⬜️            |
| [hours](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.hours.html#pyspark.sql.functions.hours)(col) | ⬜️            |
| [hypot](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.hypot.html#pyspark.sql.functions.hypot)(col1, col2) | ⬜️            |
| [initcap](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.initcap.html#pyspark.sql.functions.initcap)(col) | ⬜️            |
| [input\_file\_name](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.input_file_name.html#pyspark.sql.functions.input_file_name)() | ⬜️            |
| [instr](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.instr.html#pyspark.sql.functions.instr)(str, substr) | ⬜️            |
| [isnan](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.isnan.html#pyspark.sql.functions.isnan)(col) | ⬜️            |
| [isnull](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.isnull.html#pyspark.sql.functions.isnull)(col) | ⬜️            |
| [json\_tuple](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.json_tuple.html#pyspark.sql.functions.json_tuple)(col, \*fields) | ⬜️            |
| [kurtosis](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.kurtosis.html#pyspark.sql.functions.kurtosis)(col) | ⬜️            |
| [lag](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.lag.html#pyspark.sql.functions.lag)(col\[, offset, default\]) | ⬜️            |
| [last](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.last.html#pyspark.sql.functions.last)(col\[, ignorenulls\]) | ⬜️            |
| [last\_day](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.last_day.html#pyspark.sql.functions.last_day)(date) | ⬜️            |
| [lead](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.lead.html#pyspark.sql.functions.lead)(col\[, offset, default\]) | ⬜️            |
| [least](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.least.html#pyspark.sql.functions.least)(\*cols) | ⬜️            |
| [length](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.length.html#pyspark.sql.functions.length)(col) | ⬜️            |
| [levenshtein](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.levenshtein.html#pyspark.sql.functions.levenshtein)(left, right) | ⬜️            |
| [lit](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.lit.html#pyspark.sql.functions.lit)(col) | ⬜️            |
| [locate](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.locate.html#pyspark.sql.functions.locate)(substr, str\[, pos\]) | ⬜️            |
| [log](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.log.html#pyspark.sql.functions.log)(arg1\[, arg2\]) | ⬜️            |
| [log10](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.log10.html#pyspark.sql.functions.log10)(col) | ⬜️            |
| [log1p](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.log1p.html#pyspark.sql.functions.log1p)(col) | ⬜️            |
| [log2](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.log2.html#pyspark.sql.functions.log2)(col) | ⬜️            |
| [lower](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.lower.html#pyspark.sql.functions.lower)(col) | ⬜️            |
| [lpad](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.lpad.html#pyspark.sql.functions.lpad)(col, len, pad) | ⬜️            |
| [ltrim](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.ltrim.html#pyspark.sql.functions.ltrim)(col) | ⬜️            |
| [map\_concat](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.map_concat.html#pyspark.sql.functions.map_concat)(\*cols) | ⬜️            |
| [map\_entries](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.map_entries.html#pyspark.sql.functions.map_entries)(col) | ⬜️            |
| [map\_filter](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.map_filter.html#pyspark.sql.functions.map_filter)(col, f) | ⬜️            |
| [map\_from\_arrays](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.map_from_arrays.html#pyspark.sql.functions.map_from_arrays)(col1, col2) | ⬜️            |
| [map\_from\_entries](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.map_from_entries.html#pyspark.sql.functions.map_from_entries)(col) | ⬜️            |
| [map\_keys](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.map_keys.html#pyspark.sql.functions.map_keys)(col) | ⬜️            |
| [map\_values](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.map_values.html#pyspark.sql.functions.map_values)(col) | ⬜️            |
| [map\_zip\_with](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.map_zip_with.html#pyspark.sql.functions.map_zip_with)(col1, col2, f) | ⬜️            |
| [max](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.max.html#pyspark.sql.functions.max)(col) | ✅            |
| [md5](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.md5.html#pyspark.sql.functions.md5)(col) | ⬜️            |
| [mean](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.mean.html#pyspark.sql.functions.mean)(col) | ⬜️            |
| [min](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.min.html#pyspark.sql.functions.min)(col) | ✅            |
| [minute](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.minute.html#pyspark.sql.functions.minute)(col) | ⬜️            |
| [monotonically\_increasing\_id](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.monotonically_increasing_id.html#pyspark.sql.functions.monotonically_increasing_id)() | ⬜️            |
| [month](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.month.html#pyspark.sql.functions.month)(col) | ⬜️            |
| [months](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.months.html#pyspark.sql.functions.months)(col) | ⬜️            |
| [months\_between](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.months_between.html#pyspark.sql.functions.months_between)(date1, date2\[, roundOff\]) | ⬜️            |
| [nanvl](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.nanvl.html#pyspark.sql.functions.nanvl)(col1, col2) | ⬜️            |
| [next\_day](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.next_day.html#pyspark.sql.functions.next_day)(date, dayOfWeek) | ⬜️            |
| [nth\_value](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.nth_value.html#pyspark.sql.functions.nth_value)(col, offset\[, ignoreNulls\]) | ⬜️            |
| [ntile](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.ntile.html#pyspark.sql.functions.ntile)(n) | ⬜️            |
| [overlay](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.overlay.html#pyspark.sql.functions.overlay)(src, replace, pos\[, len\]) | ⬜️            |
| [pandas\_udf](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.pandas_udf.html#pyspark.sql.functions.pandas_udf)(\[f, returnType, functionType\]) | ⬜️            |
| [percent\_rank](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.percent_rank.html#pyspark.sql.functions.percent_rank)() | ⬜️            |
| [percentile\_approx](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.percentile_approx.html#pyspark.sql.functions.percentile_approx)(col, percentage\[, accuracy\]) | ⬜️            |
| [posexplode](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.posexplode.html#pyspark.sql.functions.posexplode)(col) | ⬜️            |
| [posexplode\_outer](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.posexplode_outer.html#pyspark.sql.functions.posexplode_outer)(col) | ⬜️            |
| [pow](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.pow.html#pyspark.sql.functions.pow)(col1, col2) | ⬜️            |
| [quarter](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.quarter.html#pyspark.sql.functions.quarter)(col) | ⬜️            |
| [radians](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.radians.html#pyspark.sql.functions.radians)(col) | ⬜️            |
| [raise\_error](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.raise_error.html#pyspark.sql.functions.raise_error)(errMsg) | ⬜️            |
| [rand](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.rand.html#pyspark.sql.functions.rand)(\[seed\]) | ⬜️            |
| [randn](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.randn.html#pyspark.sql.functions.randn)(\[seed\]) | ⬜️            |
| [rank](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.rank.html#pyspark.sql.functions.rank)() | ⬜️            |
| [regexp\_extract](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.regexp_extract.html#pyspark.sql.functions.regexp_extract)(str, pattern, idx) | ⬜️            |
| [regexp\_replace](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.regexp_replace.html#pyspark.sql.functions.regexp_replace)(str, pattern, replacement) | ⬜️            |
| [repeat](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.repeat.html#pyspark.sql.functions.repeat)(col, n) | ⬜️            |
| [reverse](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.reverse.html#pyspark.sql.functions.reverse)(col) | ⬜️            |
| [rint](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.rint.html#pyspark.sql.functions.rint)(col) | ⬜️            |
| [round](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.round.html#pyspark.sql.functions.round)(col\[, scale\]) | ⬜️            |
| [row\_number](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.row_number.html#pyspark.sql.functions.row_number)() | ⬜️            |
| [rpad](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.rpad.html#pyspark.sql.functions.rpad)(col, len, pad) | ⬜️            |
| [rtrim](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.rtrim.html#pyspark.sql.functions.rtrim)(col) | ⬜️            |
| [schema\_of\_csv](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.schema_of_csv.html#pyspark.sql.functions.schema_of_csv)(csv\[, options\]) | ⬜️            |
| [schema\_of\_json](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.schema_of_json.html#pyspark.sql.functions.schema_of_json)(json\[, options\]) | ⬜️            |
| [second](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.second.html#pyspark.sql.functions.second)(col) | ⬜️            |
| [sequence](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.sequence.html#pyspark.sql.functions.sequence)(start, stop\[, step\]) | ⬜️            |
| [sha1](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.sha1.html#pyspark.sql.functions.sha1)(col) | ⬜️            |
| [sha2](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.sha2.html#pyspark.sql.functions.sha2)(col, numBits) | ⬜️            |
| [shiftLeft](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.shiftLeft.html#pyspark.sql.functions.shiftLeft)(col, numBits) | ⬜️            |
| [shiftRight](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.shiftRight.html#pyspark.sql.functions.shiftRight)(col, numBits) | ⬜️            |
| [shiftRightUnsigned](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.shiftRightUnsigned.html#pyspark.sql.functions.shiftRightUnsigned)(col, numBits) | ⬜️            |
| [shuffle](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.shuffle.html#pyspark.sql.functions.shuffle)(col) | ⬜️            |
| [signum](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.signum.html#pyspark.sql.functions.signum)(col) | ⬜️            |
| [sin](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.sin.html#pyspark.sql.functions.sin)(col) | ⬜️            |
| [sinh](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.sinh.html#pyspark.sql.functions.sinh)(col) | ⬜️            |
| [size](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.size.html#pyspark.sql.functions.size)(col) | ⬜️            |
| [skewness](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.skewness.html#pyspark.sql.functions.skewness)(col) | ⬜️            |
| [slice](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.slice.html#pyspark.sql.functions.slice)(x, start, length) | ⬜️            |
| [sort\_array](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.sort_array.html#pyspark.sql.functions.sort_array)(col\[, asc\]) | ⬜️            |
| [soundex](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.soundex.html#pyspark.sql.functions.soundex)(col) | ⬜️            |
| [spark\_partition\_id](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.spark_partition_id.html#pyspark.sql.functions.spark_partition_id)() | ⬜️            |
| [split](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.split.html#pyspark.sql.functions.split)(str, pattern\[, limit\]) | ⬜️            |
| [sqrt](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.sqrt.html#pyspark.sql.functions.sqrt)(col) | ⬜️            |
| [stddev](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.stddev.html#pyspark.sql.functions.stddev)(col) | ⬜️            |
| [stddev\_pop](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.stddev_pop.html#pyspark.sql.functions.stddev_pop)(col) | ⬜️            |
| [stddev\_samp](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.stddev_samp.html#pyspark.sql.functions.stddev_samp)(col) | ⬜️            |
| [struct](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.struct.html#pyspark.sql.functions.struct)(\*cols) | ⬜️            |
| [substring](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.substring.html#pyspark.sql.functions.substring)(str, pos, len) | ✅            |
| [substring\_index](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.substring_index.html#pyspark.sql.functions.substring_index)(str, delim, count) | ⬜️            |
| [sum](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.sum.html#pyspark.sql.functions.sum)(col) | ✅            |
| [sumDistinct](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.sumDistinct.html#pyspark.sql.functions.sumDistinct)(col) | ⬜️            |
| [tan](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.tan.html#pyspark.sql.functions.tan)(col) | ⬜️            |
| [tanh](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.tanh.html#pyspark.sql.functions.tanh)(col) | ⬜️            |
| [timestamp\_seconds](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.timestamp_seconds.html#pyspark.sql.functions.timestamp_seconds)(col) | ⬜️            |
| [toDegrees](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.toDegrees.html#pyspark.sql.functions.toDegrees)(col) | ⬜️            |
| [toRadians](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.toRadians.html#pyspark.sql.functions.toRadians)(col) | ⬜️            |
| [to\_csv](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.to_csv.html#pyspark.sql.functions.to_csv)(col\[, options\]) | ⬜️            |
| [to\_date](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.to_date.html#pyspark.sql.functions.to_date)(col\[, format\]) | ⬜️            |
| [to\_json](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.to_json.html#pyspark.sql.functions.to_json)(col\[, options\]) | ⬜️            |
| [to\_timestamp](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.to_timestamp.html#pyspark.sql.functions.to_timestamp)(col\[, format\]) | ⬜️            |
| [to\_utc\_timestamp](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.to_utc_timestamp.html#pyspark.sql.functions.to_utc_timestamp)(timestamp, tz) | ⬜️            |
| [transform](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.transform.html#pyspark.sql.functions.transform)(col, f) | ⬜️            |
| [transform\_keys](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.transform_keys.html#pyspark.sql.functions.transform_keys)(col, f) | ⬜️            |
| [transform\_values](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.transform_values.html#pyspark.sql.functions.transform_values)(col, f) | ⬜️            |
| [translate](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.translate.html#pyspark.sql.functions.translate)(srcCol, matching, replace) | ⬜️            |
| [trim](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.trim.html#pyspark.sql.functions.trim)(col) | ⬜️            |
| [trunc](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.trunc.html#pyspark.sql.functions.trunc)(date, format) | ⬜️            |
| [udf](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.udf.html#pyspark.sql.functions.udf)(\[f, returnType\]) | ⬜️            |
| [unbase64](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.unbase64.html#pyspark.sql.functions.unbase64)(col) | ⬜️            |
| [unhex](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.unhex.html#pyspark.sql.functions.unhex)(col) | ⬜️            |
| [unix\_timestamp](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.unix_timestamp.html#pyspark.sql.functions.unix_timestamp)(\[timestamp, format\]) | ⬜️            |
| [upper](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.upper.html#pyspark.sql.functions.upper)(col) | ⬜️            |
| [var\_pop](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.var_pop.html#pyspark.sql.functions.var_pop)(col) | ⬜️            |
| [var\_samp](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.var_samp.html#pyspark.sql.functions.var_samp)(col) | ⬜️            |
| [variance](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.variance.html#pyspark.sql.functions.variance)(col) | ⬜️            |
| [weekofyear](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.weekofyear.html#pyspark.sql.functions.weekofyear)(col) | ⬜️            |
| [when](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.when.html#pyspark.sql.functions.when)(condition, value) | ⬜️            |
| [window](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.window.html#pyspark.sql.functions.window)(timeColumn, windowDuration\[, …\]) | ⬜️            |
| [xxhash64](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.xxhash64.html#pyspark.sql.functions.xxhash64)(\*cols) | ⬜️            |
| [year](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.year.html#pyspark.sql.functions.year)(col) | ⬜️            |
| [years](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.years.html#pyspark.sql.functions.years)(col) | ⬜️            |
| [zip\_with](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.zip_with.html#pyspark.sql.functions.zip_with)(left, right, f) | ⬜️            |
| [from\_avro](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.avro.functions.from_avro.html#pyspark.sql.avro.functions.from_avro)(data, jsonFormatSchema\[, options\]) | ⬜️            |
| [to\_avro](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.avro.functions.to_avro.html#pyspark.sql.avro.functions.to_avro)(data\[, jsonFormatSchema\]) | ⬜️            |


## Window

| Functions                                                                                                                                                                         | Supported ✅?                            |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- |
| [Window.currentRow](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Window.currentRow.html#pyspark.sql.Window.currentRow)                               | ⬜️ |
| [Window.orderBy](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Window.orderBy.html#pyspark.sql.Window.orderBy)(\*cols)                                | ⬜️ |
| [Window.partitionBy](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Window.partitionBy.html#pyspark.sql.Window.partitionBy)(\*cols)                    | ⬜️ |
| [Window.rangeBetween](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Window.rangeBetween.html#pyspark.sql.Window.rangeBetween)(start, end)             | ⬜️ |
| [Window.rowsBetween](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Window.rowsBetween.html#pyspark.sql.Window.rowsBetween)(start, end)                | ⬜️ |
| [Window.unboundedFollowing](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Window.unboundedFollowing.html#pyspark.sql.Window.unboundedFollowing)       | ⬜️ |
| [Window.unboundedPreceding](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Window.unboundedPreceding.html#pyspark.sql.Window.unboundedPreceding)       | ⬜️ |
| [WindowSpec.orderBy](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.WindowSpec.orderBy.html#pyspark.sql.WindowSpec.orderBy)(\*cols)                    | ⬜️ |
| [WindowSpec.partitionBy](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.WindowSpec.partitionBy.html#pyspark.sql.WindowSpec.partitionBy)(\*cols)        | ⬜️ |
| [WindowSpec.rangeBetween](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.WindowSpec.rangeBetween.html#pyspark.sql.WindowSpec.rangeBetween)(start, end) | ⬜️ |
| [WindowSpec.rowsBetween](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.WindowSpec.rowsBetween.html#pyspark.sql.WindowSpec.rowsBetween)(start, end)    | ⬜️ |

## Grouping

| Functions                                                                                                                                                                                                      | Supported ✅?                            |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- |
| [GroupedData.agg](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.agg.html#pyspark.sql.GroupedData.agg)(\*exprs)                                                         | ✅ |
| [GroupedData.apply](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.apply.html#pyspark.sql.GroupedData.apply)(udf)                                                       | ⬜️ |
| [GroupedData.applyInPandas](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.applyInPandas.html#pyspark.sql.GroupedData.applyInPandas)(func, schema)                      | ⬜️ |
| [GroupedData.avg](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.avg.html#pyspark.sql.GroupedData.avg)(\*cols)                                                          | ✅ |
| [GroupedData.cogroup](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.cogroup.html#pyspark.sql.GroupedData.cogroup)(other)                                               | ⬜️ |
| [GroupedData.count](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.count.html#pyspark.sql.GroupedData.count)()                                                          | ✅ |
| [GroupedData.max](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.max.html#pyspark.sql.GroupedData.max)(\*cols)                                                          | ⬜️ |
| [GroupedData.mean](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.mean.html#pyspark.sql.GroupedData.mean)(\*cols)                                                       | ⬜️ |
| [GroupedData.min](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.min.html#pyspark.sql.GroupedData.min)(\*cols)                                                          | ⬜️ |
| [GroupedData.pivot](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.pivot.html#pyspark.sql.GroupedData.pivot)(pivot\_col\[, values\])                                    | ⬜️ |
| [GroupedData.sum](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.sum.html#pyspark.sql.GroupedData.sum)(\*cols)                                                          | ✅ |
| [PandasCogroupedOps.applyInPandas](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.PandasCogroupedOps.applyInPandas.html#pyspark.sql.PandasCogroupedOps.applyInPandas)(func, schema) | ⬜️ |