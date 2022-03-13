# Expression Evaluation

`SparkExpressionToCExpression` manages most of the logic involved in converting plain Spark expressiont to VE
expressions. Code needs to be compiled to evaluate most expressions, and that code is linked statically to the Cyclone
library. These are fairly straightforward, and any expression evaluation is a Scala `Either[Expression, CExpression]` to
indicate either an expression that could not be converted, or a final output of a C Expression.

## `CExpression`

This is an important type, which is built from `(cCode: String, isNotNullCode: Option[String])`. cCode refers to the
inline C expression, and `isNotNullCode` establishes whether the output from this expression is not an SQL `NULL`. If
this is `None`, then the expression result is never `NULL`. This is hugely important as SQL uses `NULL` throughout, and
specific logic to handle functions like `COALESCE` needs to be implemented. Ultimately, the result of `isNotNullCode` is
used in `set_validity` calls throughout the C function evaluation.

## `DeclarativeAggregationConverter`

This is hugely important in the evaluation of Spark's aggregates, which are complex. For instance, to compute an `AVG`
in a distributed fashion, you first need to compute the `SUM()` of all the inputs, as well as the `COUNT()`, and then
across the different groups, divide the two. The possibilities are quite endless (eg `SUM(x) / (1 + AVG(y))` is entirely
possible), and to support that, `AggregateHole` is supported. Underneath the surface, it uses the `DeclarativeAggregate`
of Spark, which decomposes complex aggregations into their fundamental parts: partial aggregation, a merge, and a final
evaluation.
