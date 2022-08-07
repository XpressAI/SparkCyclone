# Spark Cyclone SQL Planning

Spark's plans are rewritten into Cyclone plans in `VeRewriteStrategy`. There are 2 kinds of plans:

- Boundary/transformation plans, such as `VectorEngineToSparkPlan` and `SparkToVectorEnginePlan`.
- Data processing plans, such as `VeOneStageEvaluationPlan` for evaluations.

The transformation plans are inserted to convert between Spark columnar batches and plans that support
`VeColBatch` as results; they in particular extend the `SupportsVeColBatch` trait/interface, which is used expressly
when you have chains of plans that execute on the Vector Engine.

One of the most special data processing plans is `VeAmplifyBatchesPlan`, intended to compress smaller data sets into
larger ones, especially following filtering and joining, to reduce the calls needed to the `VeProcess`.

Due to the distributed nature of Spark, many of the plans deal with transforming input `RDD`s via methods
like `mapPartitions`, so that data across processes is never mixed implicitly; the placement of `import` statements is
significant as well to identify the call contexts most appropriately.
