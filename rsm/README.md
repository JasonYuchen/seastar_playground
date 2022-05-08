# Rafter Replicated State Machine

## Overall Design

`TODO`

## Backpressure

Each statemachine owns a `worker` to handle all its tasks. To avoid resource contention (e.g. multiple statemachines
start taking snapshots/recovering from snapshots simultaneously), we introduce a centralized task limiter to control the
concurrent snapshot-related tasks. When we encounter a new snapshot task, either from user's request or from the
periodic snapshot config, we check the task limiter and reject requests if the system is overloaded.

`TODO`
