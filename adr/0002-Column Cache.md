# Column Cache System

## Status
PROPOSED

Proposed by: Eduardo Gonzalez

Discussed with: Samuel Audet, Dominik Wosinski

## Context

Caching data is one of the most effective ways to improve the performance of operations on Spark.  When using an 
accelerator such as a Vector Engine or a GPU, which are devices with their own memory, significant time is spent
retrieving the data from the host system's memory which has to be sent over PCIe which has lower bandwidth than both
main memory and the devices memory.

A "Column Cache" system that pre-loads data into the device memory would allow us to skip the redundant copying of
the same tables. Perhaps more importantly than the copying is the fact that we have to convert data to a representation
that is VE friendly. A significant amount of time is spent on the conversion work.


## Proposal

the prpposal is to introduce a column cache that is used to keep track of the data sent to the VE and reuse 
it when possible. the column cache system should come in 3 parts

- hook to intercept CACHE TABLE commands
- registry that keeps track of what data is on the VE memory
- copy detection algorithm that to detect of a copy already exists before we start conversion

## Consequences

the biggest consequence will be the need to keep track of free memory. The registry must keep in mind that
since we are running 8 executors, each can only use about 12% of the total memory. 

We must also keep converted copies in main memory. So we must also keep track of the executor memory as this might
cause us to require more memory than was used before.

More work will have to be done on VE to take full advantage of the cache as any CPU offloaded work results would still
have to be converted.  If our copy detection algorithm isn't efficient that will cause query slow down.

### Advantages

We have measured that 30% of the time or more is spent on converting data from Spark rows to Arrow format. The cache 
will eliminate the need to do that conversion for base tables which would be used over and over.

### Disadvantages

Also a consequence, but a disadvantage of this will be the fact that we will use more main memory than before. 

By enabling users to disable the cache by a config then we can ignore this issue.

## Discussion

< Questions/Answers and suggestions based on the above proposed items >
