# Developer Overview

# Physical plan in Aurora4Spark



Currently Aurora4Spark physical plan is a bit hard to understand since it showed C++ code generated using CEvaluationPlan

```bash
== Parsed Logical Plan ==
Aggregate [count(1) AS count#17L]
+- Repartition 1, true
   +- Relation[sid#0,age#1,job#2,marital#3,education#4] parquet
 
== Analyzed Logical Plan ==
count: bigint
Aggregate [count(1) AS count#17L]
+- Repartition 1, true
   +- Relation[sid#0,age#1,job#2,marital#3,education#4] parquet
 
== Optimized Logical Plan ==
Aggregate [count(1) AS count#17L]
+- Repartition 1, true
   +- Project
      +- Relation[sid#0,age#1,job#2,marital#3,education#4] parquet
 
== Physical Plan ==
CEvaluationPlan eval_95196397, [count(1) AS count#17L], CodeLines(
extern "C" long eval_95196397(, nullable_bigint_vector* output_0_count) {
output_0_count->data = (long *)malloc(1 * sizeof(long));
output_0_count->count = 1;
long count_counted = 0;
output_0_count->validityBuffer = (unsigned char *) malloc(1 * sizeof(unsigned char));
output_0_count->validityBuffer[0] = 1;
#pragma _NEC ivdep
for (int i = 0; i < input_0->count; i++) {
count_counted += 1;
}
output_0_count->data[0] = count_counted;
return 0;
}
), ExecutorPluginManagedEvaluator
+- Exchange RoundRobinPartitioning(1), false, [id=#30]
   +- *(1) ColumnarToRow
      +- FileScan parquet [] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex[hdfs://192.168.0.40/dotdata/projects/ckt2mmvti02he3192qkahl5js/imported_data/ck..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<>
```

