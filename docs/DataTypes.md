# Data Types in Spark Cyclone

## General

Since the VE is designed to work with data in a columnar, and thus vectorizable,
format, data in Spark Cyclone is organized around an abstraction called the column
vector.  All column vector implementations in the plugin contain the following data:

* A name for identification
* The scalar data type that make up the column vector (e.g. C `int32_t`, Scala `Float`).
* A data array of values of the designated scalar type.
* The number of elements in the column vector.
* A bitmap buffer that indicates which array entries are `NULL` values.

Column vector implementations for non-fixed-size data types (i.e. `varchar`) also
contain the following data:

* The data array size.
* An array of `varchar` lengths.
* An array of `varchar` offsets.

This aligns with the `frovedis::words` data structure, which is used to represent
`varchar` column vectors in Frovedis.


## VE Side

### C `nullable_*_vector` Types

The Cyclone C++ library defines the following column vector classes
[here](../src/main/resources/io/sparkcyclone/cpp/cyclone/transfer-definitions.hpp):

| Column Vector Type | C Scalar Type | Comments |
|--------------------|---------------|----------|
| `nullable_short_vector`   | `int32_t` | `int32_t` is used as the scalar type because the VE is not optimized for `int16_t` |
| `nullable_int_vector`     | `int32_t` | |
| `nullable_bigint_vector`  | `int64_t` | |
| `nullable_float_vector`   | `float`   | `float` is assumed to be 32 bits  |
| `nullable_double_vector`  | `double`  | `double` is assumed to be 64 bits |
| `nullable_varchar_vector` | `int32_t` | Denotes a column vector of `UTF-32` strings, where each character is represented as an `int32_t` value |

## JVM Side

### Column Vectors

On the JVM side, Spark Cyclone defines 4 different column vector implementations,
all of which serve different purposes.  All 4 implementations support the following
column vector types, which map to the C `nullable_*_vector` types when the data
is transferred into the VE:

| Column Vector Type | JVM Scalar Type | Corresponding C Column Vector Type | Comments |
|--------------------|-----------------|------------------------------------|----------|
| `VeNullableShort`  | `Short`  | `nullable_short_vector`   | Values are converted to `Int` representation internally |
| `VeNullableInt`    | `Int`    | `nullable_int_vector`     | |
| `VeNullableLong`   | `Long`   | `nullable_bigint_vector`  | |
| `VeNullableFloat`  | `Float`  | `nullable_float_vector`   | |
| `VeNullableDouble` | `Double` | `nullable_double_vector`  | |
| `VeString`         | `String` | `nullable_varchar_vector` | Values are converted to `UTF-32LE` `Array[Byte]` representation internally |

#### `BytePointerColVector`

`BytePointerColVector` describes a column vector whose data buffers are managed
by `JavaCPP` through off-heap memory `BytePointer`s.  This is the bridge data
structure through which JVM / Spark data is transferred to the VE and back, since
the JavaCPP AVEO wrapper only deals with off-heap memory pointers.

#### `VeColVector`

`VeColVector` describes a column vector whose data buffers are on the VE.  When
the contents of `BytePointerColVector` are transferred to the VE, the VE memory
locations are returned and stored as `Long` values in `VeColVector`.

#### `ByteArrayColVector`

`ByteArrayColVector` is the on-heap version of `BytePointerColVector`, where the
data buffers are stored as `Array[Byte]`.  It is used mainly for data serialization
across exchanges when `repartitionByKey()` is called on an RDD as part of executing
hash exchange or partial aggregations.

#### `UnitColVector`

`UnitColVector` contains only the description of the column vector, separately
from the data that the column vector carries.  The are used mainly for serializing
and deserializing `VeColVector` into an `OutputStream` and from an `InputStream`,
respectively, when transferring data across executor / network boundaries.

In both the case of `ByteArrayColVector` and `UnitColVector`, serialization is
un-avoidable unless a direct transport is created that transports data across
multiple VEs in Spark's fashion, but even then, this would reqiure a specialized
`RDD` implementation to deal with the distribution / checkpoint / spillover aspects
that are provided by Spark's framework.  An alternatives would be to serialize raw
`Array[Byte]` which requires double the copying and allocations, and had proven
to be much slower than direct serialization into the streams.

### Column Batches

Column vectors can be packed together into column batches.  In Spark SQL terms,
a column batch can be thought of as a table, with the column vectors representing
each column of the table.  A corresponding column batch implementation exists for
each of the column vector implementations, e.g. `BytePointerColBatch`.

### Conversions

Non-lossy conversions to `BytePointerColVector` are available for the following
types:

* `Array[T]`
* `Seq[Option[T]]`
* `org.apache.arrow.vector.ValueVector`
* `org.apache.spark.sql.vectorized.ColumnVector`

In addition, conversions are available between the 4 column vector implementations.
