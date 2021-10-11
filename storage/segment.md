# rafter storage

## overall design

The storage layer of the Raft protocol should persist the metadata and the log entries. Rafter uses the same design as
the dragonboat that the log entries are kept in both memory and the disk.

TODO: illustrate the in_memory part

TODO: illustrate the on_disk part

TODO: illustrate the snapshot organization

```text
     compacted     not in memory             in memory
 [ .......... 3 ] [ 4  5  6  7 ] [ 8  9  10  11  12  13  14  15  16 ]

 in_memory  [+++]                [++++++++++++++++++++++++++++++++++]
              |                    |      |       |       |       |
           snapshot            marker  applied  commit  saved  latest

 on_disk    [+++] [+++++++++++++++++++++++++++++++++++++++++]
              |     |                                     |
           marker first                                  last
```

## in-memory layer

## on-disk layer

### why not use KV store?

The pattern of the Raft log entries is simple: append and persist, and the log entries are marked with consecutive
numbers called log index.

A popular KV store (e.g. RocksDB) is considered too heavy since the Rafter respect the shared-nothing design in Seastar
and each Raft cluster is handled within one shard. The rich feature set in KV store including concurrency control, 
transaction, etc will not be used in Rafter.

We design and implement a naive storage layer for Rafter using write ahead log, WAL. The design mainly refers to
[etcd](), [braft](), [dragonboat]().

### basic design

All entries and hard states coming from the Raft module are serialized and appended to a segment file (WAL) with a
corresponding in-memory index tracking each entry by its location tuple `(filename, offset)`.

The WAL files are rolling with a threshold in size, a current active file has a prefix as `_inprogress` and all archived
WAL files are immutable. The name of segment is a monotonic increasing number starting from 0.

Log entry index is designed to be flexible and sparse: 1 index slot tracks 1 or more log entries within a single WAL
file by recording the following fields:

- start index of consecutive indexes
- end index of consecutive indexes
- name of the WAL file
- offset in this WAL file
- number of total bytes occupied by these indexes

With this information, we can easily merge several indexes together when necessary.

Log entry index is kept in memory and only rebuilt during crash recovery by replaying all existing segments.

```text
                     -------------- segment manager --------------
                    /                      |                      \             
                   /                       |                       \
              segment ptr              segment ptr              segment ptr
             +-----------+                 |                        | 
             |   meta    |                 |                        |
             +-----------+                 |                        |
             |inmem index|                 |                        |
             +-----------+                 |                        |
memory            |                        |                        |
------------------|------------------------|------------------------|-------------
disk(s)           |                        |                        |
               00000.log               00056.log           00129.log_inprogress
             +-----------+           +-----------+            +-----------+
             |           |           |           |            |           |
             +-----------+           +-----------+            +-----------+

rafter @ disk0
  |--<cluster_id:020d_node_id:020d>
  |    |--config
  |    |--<seq:020d>.log
  |    |--<seq:020d>.log
  |    |--<seq:020d>.log_inprogress
  |    |--...
  |--<cluster_id:020d_node_id:020d>
  |--...

rafter @ disk1
  |--<cluster_id:020d_node_id:020d>
  |--...
```

### normal flow

- **write**
  1. append/overwrite uncommitted log entries
  2. update index
  3. rolling if exceeds size threshold
  4. fdatasync
- **read**
  1. query the log entry index to find out the locations of the entries
  2. use locations to fetch the corresponding entries
- **rolling**
  1. TODO

### recovery flow

1. parse all segments to rebuild logs and indexes
2. truncate segments if any error occurs during recovery

```text
             segment
    +-----------------------+
    | 64bit length          |  <-- including the checksum
    +-----------------------+
    |  8bit checksum type   |
    +-----------------------+
    | 32bit checksum        |
    +-----------------------+
    | bytes update          |  <-- serialized update structure
    +-----------------------+
    |         ....          |
    +-----------------------+
```

### possible limitations

1. Currently, each node has its own WAL files, which simplifies the implementation but has some limitations:

   - if there are too many Raft groups, the number of file handlers maybe too large
   - each WAL needs `fsync`, large number of WAL files introduce large number of `fsync` calls

   In the future, we can have a 1 WAL module per shard scheme (like Seastar's disk scheduler) to reduce the number of 
   files and make this WAL module multiplexed.

2. The indexes are kept in the memory only, which may consume too much memory, in the future we can dump the indexes
   into files and load these index files in need.

## snapshot organization
