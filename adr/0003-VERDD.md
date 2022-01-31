# Implementation of Vector Engine Resilient Distributed Datasets (VERDD)

## Status
PROPOSED (change to ACCEPTED or REJECTED later + add date)

Proposed by: Marko Nußbaum (01/02/2022)

Discussed with: Name1, Name2, Name3, ...

## Context
Currently, the only easy way to use the SX Aurora TSUBASA Vector Engine on Apache Spark is through the provided SQL Plugin. This provides an easy and convenient way for users to make use of VE provided acceleration on a very high level.

However, one of the most fundamental data structures in Apache Spark are Resilient Distributed Datasets (RDDs), which are commonly used on more low level implementations.
RDDs allows Spark Users to run provided code (in form of Python, Java or Scala objects) in parallel on the cluster using a number of methods implemented on the RDD class, such as "map", 

## Proposal
We propose to implement a VE-enabled RDD type as follows:


// TODO: Find better, cleaner (and less labor-intensive) architecture for this case


// This does not work so well due to RDD being abstract and having subclasses:

The SparkCyclone plugin to the Apache Spark is complemented by  a VERDD<T> class that inherits from RDD<T>, thus providing a drop-in replacement for the latter from day one (i.e.: A user can just replace RDD with VERDD in her code base and gets the additional benefit of possible acceleration on nodes that feature SX Aurora TSUBASA).

Critical methods that are good candidates to make use of vectorization features like "map()", "mapParitions()", etc., will be accompanied by a vectorizing version with the "ve" postfix, i.e. "veMap()", "veMapParitions()", etc.

Methods that received a "ve" companion method also receive an additional method with postfix "nve" ("non-vectorizing") as a convenience, thatjust relays the parameters to the superclasses implementation (i.e. "nveMap()", "nveMapPartitions()", etc.). 

Then, the original methods are overriden to make use of the vectorizing versions, iff there is SX Aurora TSUBASA hardware present in the current node *and* the provided function parameter is (at least parially) vectorizable.

### Implementation of vectorization

Ideally, the users of the system might be totally ignorant to the fact, that the provided function runs on a vector engine.

// this means we need to find a way to convert a provided function to Aurora binary code...

// ideas
// - transcode scala function to c/c++ (or fortran or nas) and use nec tools to create bin
// - create compiler scala -> aurora bin (maybe a bit out of scope)
// - convert bytecode to aurora bin (would work with java, too. However, probably ooscope, too)

 // TODO





## Consequences 

### Advantages
The architecture described above allows users to:

#### Easy VE integration
By providing a drop-in replacement to the RDD class, users can get an (possibly) accelerated version of their implementation by changing just one word (i.e. replace RDD with VERDD).

#### Maintaining Control over VE usage
By using the "ve" prefixed versions of critical member functions, usage of the vector engine can be enforced, while it can be prohibited by using the "nve" prefixed versions.
Or it can be left to the implementation be using the un-prefixed version as a sensible default.

#### Mixed VE Clusters
By using only the unprefixed methods, tasks can be run in parallel on mixed clusters, where some machines do feature vector engines while others do not.

  
### Disadvantages

#### Replication

In Spark there are several subclasses of RDD, such as EdgeRDD, HadoopRDD, etc.

To be universally useful, each of these needs to be sublassed just like RDD (resulting in classes VEEdgeRDD, VEHadoopRDD,...) and if there are further sublasses in the future, these also will need to be subclassed to provide VE utilization.

There are several possiblities to implement this:
- provide the additional functionality in a helper class or trait to be used to override methods in the subclasses, i.e.:

```
trait Vectorize {
  def isVeAvailable() ...
  def veMap(scala.Function<T,U> f, scala.reflect.ClassTag<U> evidence$3)
	...
}

class VEDD extends RDD with Vecotrize {
	...
	override def map(...) {
		if (isVeAvailable()) {
			veMap(...)
		} else {
			super.map(...)
		}
}
```


- Proxy Pattern
```
class ProxyRDD<T> extends RDD<T> {
	
	private var rdd : RDD<T>
	
	


```




- Forking Spark
Allows to change the base class RDD, so all subclasses inherit the VE behavour.



#### Name Clashes
In the (hopefully) rare case of a user having derived from RDD and using the same method names as we do, fruther adaptaions are needed to make use of the plugin.

## Discussion

