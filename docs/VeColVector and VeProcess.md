# VeColVector and VeProcess

`VeProcess` is the general interface to interacting with the Vector Engine. It has the following key functionalities:

- Low-level functions to manage VE memory, and transfer from/to the VE and the VH (Vector Host), including integration
  with `java.io.InputStream` and `java.io.OutputStream`.
- Higher level functions to call methods (by name) in libraries that are loaded on the Vector Engine,
  taking `VeColVector`s as inputs and producing new ones as outputs. These also wrap lower level methods provided by the
  AVEO API, such as argument setting, memory allocation, and similar. The argument types need to correspond to what is
  in the underlying functions.

Technically, `VeProcess` could have anything behind the scenes, including an x86 CPU implementation, however this has
not been strictly needed in the development. In the plug-in lifecycle, `VeProcess` is created using
[`veo_proc_create`](https://www.hpc.nec/documents/veos/en/aveo/group__veoapi.html#ga6193ffeb9a3ac5c34ce666ea652e6daa),
and there should only really be one of this process instance per JVM process, in relation to the capabilities of AVEO.
A `DeferredVeProcess` is created in order to deal with Spark's serializability issues and the need to lazy-load. In unit
tests, on the other hand, the `VeProcess` is created via the functionality in `WithVeProcess`, in order to reduce the
test run times by not having to bring up a new `SparkContext`.

Test suites extending `WithVeProcess` generally put the `VeProcess` through its paces. For example, `ArrowTransferCheck`
verifies multi-function calls (eg turn one columnar batch into multiple, for the purpose of partitioning), and also
validating all the different conversions from Arrow types to the VE and back. `VeSerializerSpec` is particularly
important, and serialization will be discussed later.

`VeColVector` builds on top of `GenericColVector[Long]`, where `Long` refers to the memory location for this VE process.
A `GenericColVector` is used as there are multiple representations of a vector depending on the context, especially
across VE, VH and Spark boundaries. `GenericColVector`'s primary constituents are a count of items (`numItem`), the
`veType` to describe what is inside it and then the `buffers` to point to the content. This reflects closely to Arrow's
data storage format, in particular with buffers, which usually have at minimum a validity buffer and a data buffer, but
for the case of Varchar (String), there are extra considerations as it has variable with rather than the fixed width of
scalar types. The reason for using Arrow is that Spark's Arrow tooling was the most ready to utilize when developing the
project. There may be other formats that could be considered but this was the most straightforward to work with. As a
result, types like `nullable_varchar_vector` have been developed in the C++ Cyclone library.

`BytePointerColVector` is used to describe a vector which is currently in the off-heap memory, managed by JavaCPP. It is
typically utilized when bringing data back from the VE onto the VH, before then converting it into Arrow's Vectors. It
can also be converted into `ByteArrayColVector` for caching directly on-heap using Java's `byte[]` / Scala's
`Array[Byte]`. Lastly, we have `UnitColVector` which is a plain description of the vector, for use cases like
serializing this object in and out, separately from the data that it is carrying. The most particular use case of this
in serializing a `VeColBatch` into an `OutputStream`, and getting one back out from an `InputStream`, when transferring
data across executor/network boundaries, due to the distributed nature of Spark (see `VeSerializationStream` and
`VeDeserializationStream`). Serialization is unavoidable unless a direct transport is created that transports data
across multiple VEs in Spark's fashion, but even then, this would reqiure a specialized `RDD` implementation to deal
with the distribution/checkpoint/spillover aspects that are provided by Spark's framework. Alternatives would be to
serialize raw `Array[Byte]` which require double the copying and allocations, and had proven to be much slower than
direct serialization into the streams.

`BytePointerColVector.fromColumnarVectorViaArrow` provides a capability to convert from Spark's `ColumnarVector`. This
type is a little troublesome to deal with because it is an open and has potential Arrow, On Heap and Off Heap
implementations. To assist with some of these conversions, `TypeLink` is created. `TypeLink` is there as the types in
Spark land are different from types in the Arrow land which are also different to those on the VE land. One notable
exception, for instance, is the `Short` type which is not optimized for the VE, so has to be considered an `int` in the
native land.
