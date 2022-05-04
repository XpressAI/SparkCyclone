# Packed Transfers

## Status
PROPOSED

Proposed by: Paul Dubs (04/05/2022)

Discussed with: Eduardo Gonzalez

## Context
Achieving the highest benefit from using an accelerator like the Vector Engine requires reducing its overhead.

In order to be able to use the Vector Engine in the first place, we need to move the data that we want to work on into its memory.

As the AVEO benchmarks show, moving more data per transfer request usually results in the highest throughput, reducing the overhead of memory transfers.

However, Spark Cyclone expects to find multiple individual memory allocations for every `NullableScalarVec<T>` or `nullable_varchar_vector`.

## Proposal
We set up a transfer buffer consisting of a descriptive header and all to-be-transferred data for every transfer.

Before a transfer, we copy all data into that buffer. Then, we transfer it with a single synchronous write call. Next, the Vector Engine unpacks the received buffer to satisfy the assumption of individual memory allocations. Finally, the unpacking operation's result, a list of pointers, is transferred back to allow the Vector Host to build its data structures.

The unpacking process also merges the data of multiple batches while it unpacks the buffer. Merging multiple batches during the transfer handling results in less clean up work for follow-up processes and more data for vectorized processing.

### Transfer Buffer
The transfer buffer consists of the header followed by the actual data.

The header contains metadata about the data contained within the buffer.

```
[header size][batch count][column count][column descriptor]...[data]
```

It consists of two parts: The base header and a series of column descriptors.

The base header consists of three fields:
1. `Header size` is the number of bytes the entire header uses
2. `Batch count` is the number of batches included in this transfer
3. `Column count` is the number of columns within each batch

A series of `n` column descriptors follow the base header (`n = batch count * column count`). Every set of `batch count` column descriptors describes the entire data of a single column, i.e. the series of column descriptors is organized in a column batch-wise order.

There are two different types of column descriptors.
```
[column type][element count][data size][validity buffer size]
```
Scalar column descriptors have four fields:
1. `Column type` is an integer value between 0 and 4. It describes the column's exact scalar data type (`0`: short; `1`: int; `2`: long; `3`: float; `4`: double).
2. `Element count` specifies how many elements are in this particular column.
3. `Data size` specifies the size of this column's data buffer in bytes.
4. `Validity buffer size` specifies the size of this column's validity buffer in bytes.

```
[column type][element count][data size][offsets size][lengths size][validity buffer size]
```
Varchar column descriptors have six fields:
1. `Column type` is a fixed integer value: `5`.
2. `Element count` specifies how many elements are in this particular column.
3. `Data size` specifies the size of this column's data buffer in bytes.
4. `Offsets size` specifies the size of this column's offsets buffer in bytes.
5. `Lengths size` specifies the size of this column's lengths buffer in bytes.
6. `Validity buffer size` specifies the size of this column's validity buffer in bytes.

After the header, the actual data follows at 8-byte aligned memory locations. This alignment is necessary for vectorized code to be able to process it. Placing data at 8-byte boundaries in the transfer buffer ensures appropriate alignment after transfer but may require additional padding for data that does not naturally fit.

### Output buffer
The output buffer consists of `n` pointers (`n = column count * buffer count for each column`). These pointers are precisely the additional information the host needs to build the data structures the rest of the application needs.

The host knows the column arrangement order from creating the transfer buffer. It can, therefore, interpret this series of raw pointers without additional metadata.

Scalar columns produce three pointers: `NullableScalarVec<T>` pointer, data pointer, validity buffer pointer.

Varchar columns produce five pointers: `nullable_varchar_vector` pointer, data pointer, offsets pointer, lengths pointer, validity buffer pointer.

## Consequences 

### Advantages
* Faster transfer speeds due to reduced latency and higher throughput 
* Merging data during transfer handling removes the need for an additional merging step afterwards
  
### Disadvantages
* Multiple data copies are necessary: higher memory usage both on the host and on the Vector Engine
* Additional processing is necessary to unpack transferred data into the expected format
