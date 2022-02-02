# Implementation of Vector Engine Resilient Distributed Datasets (VERDD) using Scala Reflection


## Status
PROPOSED (change to ACCEPTED or REJECTED later + add date)

Proposed by: Marko Nußbaum (01/02/2022)

Discussed with: -

## Context
Currently, the only easy way to use the SX Aurora TSUBASA Vector Engine on Apache Spark is through the provided SQL Plugin. This provides an easy and convenient way for users to make use of VE provided acceleration on a very high level.

However, one of the most fundamental data structures in Apache Spark are Resilient Distributed Datasets (RDDs), which are commonly used on more low level implementations.
RDDs allows Spark Users to run provided code (in form of Python, Java or Scala objects) in parallel on the cluster using a number of methods implemented on the RDD class, such as "map", 




## Proposal

We propose to implement a VE-enabled RDD type in such a way, that users can make use of them with next-to-no changes on their code base.

RDDs are commonly aquired from a SparkContext sc by 2 operations:

  1. sc.*File(...)
  2. sc.parallelize(...)

Both return an object of type RDD<...>.

The returned RDD is wrapped in the new type VERDD as follows:

```
	val rdd = sc.parallelize(data) 
	val verdd = VERDD(rdd)
```

or shorter:

```
	val rdd = VERDD(sc.parallelize(data))
```

## Consequences 
While this design decision makes using the vector engine quite easy for the users, the implementation is far from trivial.
To be able to generate binary code that runs on the vector engine from the user provided Scala code, the provided functions need to be intercepted and translated into a to be defined non-Scala representation. Further design decisions about the intermediate format, the translation process and finally the creation of the binary code will have to be made before this proposal can be accepted. 


### Advantages

#### Easy VE integration
By providing a drop-in replacement to the RDD class, users can get an (possibly) accelerated version of their implementation by only minor changes to their code base. This most certainly will help with the adoption of the VERDD into the indiviual Spark landscape of users.

#### Maintaining Control over VE usage
By using the original RDD that is encapsuled in the VERDD type, users can selectively opt out of using the vector engine for sinlge operations.

### Disadvantages

#### Feasibility
At this point it is unclear, if implementing the API described above is feasible with current restrictions in place (i.e. Scala V2). Further investigation is needed before accepting this proposal.

#### Effort
Even if the implementation is feasible, it is non-trivial and might need to rely on beta-features of Scala itself (e.g. macros). But even if it is doable, a later move to Scala 3 might need some extra effort.


## Discussion

