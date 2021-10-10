# rafter storage

## overall design

The storage layer of the Raft protocol should persist the metadata and the log entries. Rafter uses the same design as the dragonboat that the log entries are kept in both memory and the disk.

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

Since the log entries are indexed by consecutive `(term, index)` pairs, the rafter uses segmented log files to serve the Raft protocol with in-memory indexes to accelerate the lookup.

Each segment contains metadata in header, log entries and an index in the tail. Only one active segment with a prefix `.log_inprogress` is writable and the remaining segments are archived and immutable. The index is a naive offset vector `[(term_1, offset_1), (term_2, offset_2), ...]` since all Raft log entries have consecutive indexes.

The active segment will be archived only when its size exceeds threshold **AND** the commit index surpasses the latest index of the segment, which ensures that the archived segments will not be overwritten. 

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
               00001.log               00056.log           00129.log_inprogress
             +-----------+           +-----------+            +-----------+
             |           |           |           |            |           |
             +-----------+           +-----------+            +-----------+
```

The files are organized as follows:

```text
rafter @ disk0
  |--<cluster_id:020d_node_id:020d>
  |    |--config
  |    |--<start_index:020d>.log
  |    |--<start_index:020d>.log
  |    |--<start_index:020d>.log_inprogress
  |    |--...
  |--<cluster_id:020d_node_id:020d>
  |--...

rafter @ disk1
  |--<cluster_id:020d_node_id:020d>
  |--...
```

A segment basically contains header (metadata), payload (entries), index as follows:

**The basic flow**: 

1. (optional) append/overwrite uncommitted log entries
2. update header
3. (optional) rolling
4. fdatasync

**The recovery flow**:

1. parse active segment from header to index and truncate if necessary
2. reconciliation to ensure all parts are consistent (e.g. the latest index in header should be equal to the last valid log entry)
3. (optional) rolling
4. (optional) fdatasync if any data reconciliation occurs

```text
             segment
    +-----------------------+
    | 64bit latest term     |  <-- Raft hard state
    +-----------------------+
    | 64bit latest index    |
    +-----------------------+
    | 64bit latest vote     |  <-- Raft hard state
    +-----------------------+
    | 64bit latest commit   |
    +-----------------------+
    | 64bit index offset    |  <-- filled when index is dumped
    +-----------------------+
    |  8bit checksum type   |
    +-----------------------+
    | 32bit header chksum   |
    +-----------------------+
    |         ~4KiB         |  <-- reserve for O_DIRECT alignment
    +-----------------------+
    | 64bit log term        |  <-- log entry, Raft hard state
    | 64bit log index       |
    |  8bit log type        |
    |  8bit checksum type   |
    | 32bit data length     |
    | 32bit data chksum     |  <-- the checksum of data
    | 32bit header chksum   |  <-- the checksum of previous fields
    | bytes data            |
    +-----------------------+
    |        ......         |
    +-----------------------+  <-- exceeds segment size threshold & commit index, archive this and prepare for rolling
    |         index         |  <-- index is dumped at last
    +-----------------------+
```

## snapshot organization
