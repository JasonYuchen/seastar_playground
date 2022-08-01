# Tips

## 1 Optimization

### 1.1 Raft

#### 1.1.1 Observer

Use [observer](https://github.com/JasonYuchen/notes/blob/master/raft/04.Cluster_Membership_Change.md#1-%E6%96%B0%E6%9C%8D%E5%8A%A1%E5%99%A8%E8%BF%BD%E8%B5%B6-catching-up-new-servers)
to smoothly introduce a new member to the cluster.

#### 1.1.2 Witness

Use [witness](https://github.com/JasonYuchen/notes/blob/master/raft/11.Related_Work.md#%E5%87%8F%E5%B0%91%E8%8A%82%E7%82%B9%E6%95%B0reducing-number-of-servers-witnesses)
to reduce election time and improve availability.

### 1.2 Node Management

#### 1.2.1 Quiesce

#### 1.2.2 Failure-domain Level Failure Detection

#### 1.2.3 Shard Awareness

#### 1.2.4 Gossip

#### 1.2.5 Feedback-based IO Scheduling

All writes are priority-based since we usually want the latency of front-end request as low as possible at the cost of a
slow back-end snapshotting/compaction.

When the system is under heavy load, the front-end operation will be given higher priority via
a [feedback-based controller](https://github.com/JasonYuchen/notes/blob/master/seastar/Dynamic_Priority_Adjustment.md).

### 1.3 General Techniques

#### 1.3.1 Run-to-complete

#### 1.3.2 Double-buffering

#### 1.3.3 Rate Limiter

#### 1.3.4 Circuit Breaker
