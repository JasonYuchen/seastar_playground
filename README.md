# rafter

## Introduction

rafter (*in progress*) is a pipelined asynchronous multi-raft group library with an in-memory key-value replicated state
machine based on seastar.

rafter is deeply inspired by

- [dragonboat](https://github.com/lni/dragonboat)
- [etcd](https://github.com/etcd-io/etcd)
- [braft](https://github.com/baidu/braft)
- [scylla](https://github.com/scylladb/scylla)
- [seastar](https://github.com/scylladb/seastar), insights
  available [here](https://github.com/JasonYuchen/notes/tree/master/seastar)

![rafter](rafter.drawio.png)

## Build & Setup

- prerequisite
    - Compiler with C++20 support, tested with clang-12
    - Seastar, *the installation of Seastar is not trivial, please refer to
      this [post](https://github.com/JasonYuchen/notes/blob/master/seastar/Setup.md)*
    - GoogleTest, taken care of by rafter's cmake configuration

```shell
TBD
```

*it is recommended to use [ninja](https://github.com/ninja-build/ninja), [mold](https://github.com/rui314/mold) (
and [ccache](https://github.com/ccache/ccache) if you want to play with the source code) to accelerate build.*

## Roadmap

1. finalize all modules with extensive tests
2. provide detail docs and notes
3. prepare a docker image for build and demo
4. chaos tests (maybe jepsen tests)
5. benchmark and performance optimization

## Module

### Node Host

The nodehost manages and schedules all raft groups, storage, transport and replicated state machines.

TBD

### Raft Core State Machine

The [core](core/README.md) module implements the raft algorithm based on the etcd's design.

1. ⭕design doc
2. ✔️election with prevote
3. ✔️leader lease
4. ✔️leadership transfer
5. ⚠️log compaction
6. ⚠️snapshot
7. ✔️single membership change
8. ⭕joint consensus
9. ✔️observer
10. ✔️witness
11. ✔️quiesce
12. ✔️linearizable semantics

### Storage

The [storage](storage/README.md) module works as the WAL of the raft groups, managing the persistent log entries as well
as the snapshots and the configurations of the clusters.

1. ✔️design doc (lack in-memory log explanation)
2. ✔️segment, a WAL unit
3. ✔️index, for indexing raft logs (lack dump&load)
4. ✔️segment_manager, implementation of logdb interface for managing raft persistent logs

### Transport

The [transport](transport/README.md) module based on the seastar's rpc framework works as a messaging service for
clusters.

1. ⚠️design doc
2. ✔️exchanger, for exchanging raft messages
3. ✔️registry, for group discovery and peer addresses management
4. ⚠️express, for sending/receiving replicated state machine's snapshot

### Replicated State Machine

The [rsm](rsm/README.md) module bridges the user-defined replicated state machine (e.g. an in-memory key-value
store) and the Raft Core State Machine (also Storage, Transport), managing the lookup, update operations as well as
snapshot taking, recovering actions of the user's state machine.

A naive in-memory key-value storage is provided to demonstrate the interactions between a user
defined replicated state machine and the underlying framework.

1. ⭕design doc
2. ⚠️session, for idempotent operation
3. ⚠️session manager, for managing sessions
4. ⚠️snapshotter, for managing snapshot save/load operations
5. ⚠️statemachine_manager, for bridging the execution engine and the user's statemachine

### Misc

Various utilities to support different modules of the rafter.

1. ✔️backoff, retry in coroutine style
2. ✔️buffering_queue, double-buffering queue
3. ⚠️circuit_breaker
4. ✔️fragmented_temporary_buffer, for aligned (de)serialization
5. ✔️lru, lru cache
6. ⚠️rate_limiter
7. ✔️serializer, for raft message's (de)serialization
8. ✔️worker, double-buffering multi-producer single-consumer worker coroutine, building block for pipelined service in
   rafter

### Test

The [test](test/README.md) module hacks the interface of the [GoogleTest](https://github.com/google/googletest) to
support testing an asynchronous application with GoogleTest's style.

1. ✔️base, for integrating the coroutine and the gtest macros, and the Seastar's reactor engine management
2. ⚠️unit tests
3. ⭕integration tests
4. ⭕benchmark

## Doc

Some [tips](doc/tips.md) available.

in progress

## License

[Apache License 2.0](LICENSE)
