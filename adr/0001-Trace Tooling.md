# Implementation of Tracing/Monitoring

## Status
IMPLEMENTED

Proposed by: Dominik Wosinski

Discussed with: Eduardo Gonzalez

## Context
We need a way to be able to track and profile execution on JVM, but more importantly on VE side. There are some tools for profiling spark, but those are user oriented not developer oriented and they definetely won't allow us to profile and monitor execution on VE.

## Proposal
It seems that just as RAPIDS we need to implement custom profiling tool to allow tracking execution and gathering statistics about that.

The idea is to simplify the VE side as much as possible. To achieve that, from VE side we are going to publish messages only to UDP port. Those messages will then be received, aggregated and published on JVM side.

To reduce the work needed and maximize the usability the idea is to use common format for messages known from `Zipkin` and
described [here](https://zipkin.io/zipkin-api/).


For Scala side we should be able to incorporate `Zipkin` library for publishing of messages.

## Consequences

### Advantages

- Reducing the work required on VE side
- Using common format means that we should be able to use existing tools to send messages from Scala or present results.
- Tracing will allow us to monitor and optimize queries from VE side.
### Disadvantages

- UDP means that we don't have delivery guarantees.
- Every executor will gather and publish the data separately, so it may be tricky to properly coordinate and present aggregated results.
## Discussion
< Questions/Answers and suggestions based on the above proposed items >
