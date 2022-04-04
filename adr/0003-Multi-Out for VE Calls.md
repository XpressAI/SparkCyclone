# Multi-Out for VE Calls

## Status
PROPOSED

Proposed by: Paul Dubs (03/04/2022)

Discussed with: Eduardo Gonzalez

## Context
Calling functions on the VE with AVEO requires us to setup a call stack. This 
call stack contains values and pointers for inputs as well as pointers for 
outputs. The called method will write its results to the given pointer
locations and return a status as an int.

After the call, AVEO will take care of copying the values back from the VE to
the VH. *But,* it will only copy as much data as was configured during the call
stack setup.

This leaves us in a bind: If we want to return an arbitrary count of results,
we can either allocate way too much space for that (with a limit of up to about
64MB), or if we want to try and stay within reasonable limits we limit the
maximum return count.

At the time of this proposal, we are using the second option in `executeMulti`.
We limit the maximum count of outputs to 64.

## Proposal
### General Case
We use a layer of indirection and put two pointers on the call stack. The first
pointer receives the output count ("count pointer"), and the second pointer 
receives the memory location of the actual output data ("data pointer").

We use those values to then manually (`read_mem`) copy the actual data from VE
to VH.

### Specific Case: executeGrouping
In the groupBy case, we do not need the count pointer, as we receive the count
through other means: The groups_out vector gives us both the count and the group
keys.


## Consequences 

### Advantages
* We can return arbitrary counts of data without allocating and transferring 
  more data than necessary
  
### Disadvantages
* It is more manual effort to manage both the data transfer and the resources
  for this indirect approach.
