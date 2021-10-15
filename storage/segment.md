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

A popular KV store (e.g. RocksDB) is considered too heavy since the pattern is simple and the Rafter respect the
shared-nothing design in Seastar (each Raft cluster is handled within one shard). The rich feature set in KV store such
as concurrency control, transaction, etc will not be used in Rafter.

We design and implement a naive storage layer for Rafter using write ahead log, WAL. The design mainly refers to
[etcd](https://github.com/etcd-io/etcd), [braft](https://github.com/baidu/braft), 
[dragonboat](https://github.com/lni/dragonboat).

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

### further considerations

1. **one WAL for one Raft node**
   - the design and implementation are easier and more straight forward (pro)
   - indexes of same Raft node can be merged to reduce memory usage (pro)
   - isolation among Raft nodes, no need to coordinate compaction (pro)
   - if too many Raft groups are bootstrapped, the number of segments could be huge (con)
2. **one WAL for one shard** (Seastar use thread-per-shard/core design) with multiple Raft nodes
   - the total number of segments is small (pro)
   - have to coordinate Raft nodes since the WAL is shared among all Raft nodes within one shard (con)
   - the log entries of a Raft node is not consecutive, but so long as we are not frequently accessing the on-disk entries, it should not be an issue (?)
   - if we restart the system with different number of shards, we need to reshard the existing segments (con)
   
   Some notes from commitlog design in ScyllaDB (the flow of replay commitlog):
   1. list and reshard all existing segments
   2. replay logs in each new shard
   3. apply the mutations in target shard using `invoke_on` (i.e. the log replayed in new shard 1 may contain mutations belong to a foreign db in shard 3)
   4. flush all memtables to disk to make the mutations durable
   5. delete existing segments
   see `/main.cc: replaying commit log` and `/db/commitlog/commitlog_replayer.cc: db::commitlog_replayer::recover` for more details

   If Rafter use this way, then we should (changing shards is not allowed for now):
   1. list and reshard all existing segments
   2. replay logs in each new shard
   3. send the Raft entry to target shard to reconstruct segment using `invoke_on`
   4. trigger snapshot and compaction to retire existing segments
   5. delete existing segments
3. **sharding WALs for one shard**, N Raft nodes -> M WALs -> S shards
   - the total number of segments is small (pro)
   - have to coordinate (con)
   - still have to reshard if M WALs changed (con)
   - no need to reshard if Seastar's shard changed (pro)
   - maybe too complicated to implement and manage (con)

### normal flow

*CAUTION: though all operations are handled in one thread/shard, there can still be data race among coroutines*

- **write**
  1. append log entries, never overwrite
  2. update index
  3. rolling if exceeds size threshold
  4. fdatasync
- **read**
  1. query the log entry index to find out the locations of the entries
  2. the index should block all requests with index < snapshot index even the segment may still exist
  3. use locations to fetch the corresponding entries
- **rolling**
  1. create new segment
  2. fdatasync
- **compaction**
  1. new snapshot is available, update the snapshot index in the index to block subsequent requests with start index <
     snapshot index, like a read barrier
  2. release segments with end index <= snapshot index, the release should start from small index and pause when the 
     segment is currently being read
  
### recovery flow

1. block all requests as the storage layer is being recovered
2. parse all segments to rebuild logs and indexes
3. truncate segments if any error occurs during recovery
4. compact obsolete segments

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
    
only have to persist these fields in update:
    - group_id
    - state
    - entries_to_save
    - snapshot
```

### possible limitations

1. Currently, each node has its own WAL files, which simplifies the implementation but has some limitations:

   - if there are too many Raft groups, the number of file handlers maybe too large
   - each WAL needs `fsync`, large number of WAL files introduce large number of `fsync` calls

   In the future, we can have a 1 WAL module per shard scheme (like Seastar's disk scheduler) to reduce the number of 
   files and make this WAL module multiplexed.
2. The indexes are kept in memory only, which may consume too much memory, in the future we can dump the indexes
   into files and load these index files in need.
3. The segment files are created in need, in the future we can use pre-allocation to reduce the average cost.

## snapshot organization
