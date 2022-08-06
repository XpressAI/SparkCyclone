# Spark Cyclone Plugin Program Flow

The general flow is as such:

1. The Driver and Executor plug-ins are launched
2. Each Executor plug-in spawns its own VE process
3. SQL queries pass through `VeRewriteStrategy` to capture any plans that coule be processed by the plug-in; expression
   evaluation happens here.
4. `CombinedCompilationColumnarRule` compiles all the generated C++ sources using `NCC` into a `.so`, and rewrites all
   the plans to reference the new `.so` that is located on the driver.
5. Spark eventually calls a `VectorEngineToSparkPlan` to produce an `RDD` of data, which
   calls `SupportsVeColBatch#executeVeColumnar` of its child plan, and its child plan does the same. During this
   calling, `VeProcess` of the executor is summoned. Data is freed at the earlist possible opportunity or at Task
   completion.

## Compilation lifecycle

The Spark Cyclone plugin will translate your Spark SQL queries into a C++ kernel to execute them on the Vector Engine.
Compilation can take anywhere from a few seconds to a couple minutes.  While insignificant if your queries take hours
you can optimize the compilation time by specifying a directory to cache kernels using the following config.

