# Spark 2.3 compatibility

- Arrow 0.8 --> 2.x
- ExecutorPlugin, DriverPlugin || lifecycle
- Transfer/RPC of library info
- How to read batches properly? ColumnarBatch exists however `executeColumnar()` does not exist; CB is available only via WSCG for which we have some code already.
- Extensions --> works much better here

## Stages

1. Make `compile` work
2. Make `Test / compile` work
3. Make tests pass
4. Make benchmarks work
