# JVM - VE Interface

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
