# rafter

## overall

rafter (*in progress*) is a pipelined asynchronous multi-raft group library with an in-memory key-value replicated state machine based on seastar.

rafter is deeply inspired by

- [dragonboat](https://github.com/lni/dragonboat)
- [etcd](https://github.com/etcd-io/etcd)
- [braft](https://github.com/baidu/braft)
- [scylla](https://github.com/scylladb/scylla)
- [seastar](https://github.com/scylladb/seastar), insights available [here](https://github.com/JasonYuchen/notes/tree/master/seastar)

![rafter](rafter.drawio.png)

## execution

1. ❌design doc

## storage

[design](storage/README.md)

1. ✔️design doc (lack in-memory log explanation)
2. ✔️segment, a WAL unit
3. ✔️segment test
4. ✔️index, for indexing raft logs (lack dump&load)
5. ⚠️index test (lack group index test)
6. ✔️segment_manager, for managing raft persistent logs
7. ⚠️segment_manager test

## transport

[design](transport/README.md)

1. ⭕design doc
2. ⚠️exchanger, for exchanging raft messages (lack snapshot support)
3. ⚠️exchanger test
4. ⭕registry, for group discovery and peer addresses management
5. ⭕registry test
6. ⭕express, for sending replicated state machine's snapshot
7. ⭕express test

## misc

1. ✔️fragmented_temporary_buffer, for aligned (de)serialization
2. ✔️serializer, for raft message's (de)serialization
3. ✔️backoff, retry in coroutine style
4. ✔️worker, double-buffering multi-producer single-consumer worker coroutine, building block for pipelined service in rafter
