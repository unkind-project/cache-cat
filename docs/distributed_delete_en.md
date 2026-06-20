# Clock Design

## Problem Description

Cache-cat aims to implement a Raft-based version of Redis. Consider two Redis commands:

set key value px [fixed expiration time]

Destroy the value after a fixed expiration time.

set key value nx

Set the value if the key does not exist; do nothing if the key already exists. (There are many similar RMW commands in Redis; this is just an example.)

If this command is executed on both the primary and replica nodes, due to clock skew between the primary and replica, the expiration time for this key-value pair will differ on the two nodes.

The time on the primary and replica nodes cannot be synchronized. Therefore, the actual expiration times on the replica and primary are also different. If the primary has already expired the key but the replica has not, and a SET NX command arrives, it will produce different results on the two nodes. The data on the two nodes will then become permanently inconsistent.

This is because any clock operation can be seen as a system call, but system calls introduce instability. In Raft's state machine iteration, the same initial state machine is required, and each iteration must depend on the previous state machine iteration. Introducing time means that the state machine iterations on both sides depend on uncertain factors.

## Basic Assumptions and Background

> Clock offset and clock drift are different concepts.
>
> Clock offset: The time difference between two clocks at a given moment.
>
> Clock drift: The deviation of a clock's running speed relative to ideal time — i.e., whether it runs "fast or slow."

Cache-cat's overall instruction throughput exceeds 300,000 per second, using the Raft protocol.

Furthermore, in the absence of failures, a log can typically be committed within milliseconds.

For the timed expiration of data, we typically allow a deviation of several tens of milliseconds. (After all, machines themselves develop clock drift over long periods of operation.)

For reads, cache-cat already uses lease-based reads to optimize read performance. Therefore, we assume bounded clock drift. Lease-based reads are very common in TiDB, ETCD, etc.

Additionally, the design must ensure strong consistency in external semantics (consistent with Raft design, exposing only the semantics of a single state machine). Scenarios such as setting a key to expire, then reading the key as expired on the first read and finding it still exists on the second read must not occur.

Reading from replica nodes can only achieve eventual consistency, so strong consistency for reads from replicas does not need to be considered.

## Reference Solutions

> Since cache-cat uses the Raft algorithm, all the following solution designs have been adapted for Raft.

### Deterministic Instruction Approach

This approach would cause atomicity issues for instructions. Therefore, it is not considered.

First, read the current state machine state from the primary node. Generate a deterministic instruction. Then submit the deterministic instruction directly. For example, convert set nx directly into a deterministic set instruction.

However, after the read operation, the state machine may be modified by other logs (the only thing that can modify the state machine is the application of Raft logs), so the atomicity of the instruction is compromised. If two set nx instructions are executed simultaneously, both could succeed. This only works if, after the read, the current operation is the first to be committed.

### Initiate Additional Consensus on Deletion

> ETCD, Zookeeper, Consul, etc., all use this design approach for timed deletion. However, they are not designed for caching and do not require frequent deletion operations.

Every time a timed deletion occurs, a Raft consensus must be initiated. The primary node pushes the deletion to the replica nodes. Only after consensus is reached can the deletion be completed.

When data that has already been deleted is read (corresponding to Redis lazy deletion), cache-cat must first initiate a consensus to delete the data before responding to the read request, to avoid consistency issues. This increases read latency.

> In Redis, this is not a problem because commands sent by the primary to the replica do not require a response; they are streamed directly to the replicas without waiting for the replicas to finish execution before responding to the client (no backpressure mechanism). However, in Raft, completing AppendEntries requires most nodes to persist and respond, which slows down the response to the client.

Disadvantages: Greatly impacts performance. Setting expiration times is a high-frequency operation in caching systems. Although this operation is initiated by the primary node itself rather than by the client, saving one RTT, it could nearly double the number of logs that need to be committed by Raft, significantly affecting overall throughput.

Optimization: When performing periodic deletion, multiple instructions can be batched into a single operation and submitted together to reduce the performance overhead.

### Lamport Clock Approach

Since absolute clocks do not exist, the clock itself is also treated as a deterministic parameter. The primary node, every 10ms, **submits its current local time as a log, serving as a logical time**.

Both timed deletion and lazy deletion logic are determined based on this clock. That is, all expiration times do not refer to the local, unstable relative clock, but rather to the logical time determined by the consensus algorithm.

This eliminates all uncertainty, and the final result is guaranteed to be deterministic. This approach also effectively prevents clock rollback.

Drawbacks: There will be a偏差 in expiration times. Frequent time submissions mean the machine has to work continuously even when idle. When idle, there would still be 100 log submissions to disk per second.

Interestingly, when the primary node has logs continuously being submitted to replicas, the primary node's heartbeat can be omitted. By default, Raft automatically sends a heartbeat to replicas every 50ms.

## Clock Consensus Scheme

This is a self-developed solution and is the one ultimately adopted by Cache-cat.

> Based on research, infrastructure like ETCD, Zookeeper, and Redis does not assume that the time offset between primary and replica nodes is bounded.
>
> However, TiDB's PD cluster and CockroachDB make similar implicit assumptions (because they use the HLC scheme).

Here, we still assume that the time offset between the primary and replica nodes is bounded. Additionally, within this time offset range, it is impossible to elect a new leader. For example, this time offset is less than 1 second.

> Leader election time > Clock offset time

When performing a write operation (an operation that requires submitting a log via Raft), the primary node generates a write-logical timestamp for the current operation using its local time. The timestamp is `max(write_logical_time, read_logical_time, physical_time)`. All operations carry this timestamp. When a replica node receives the log, it uses this timestamp to update its own local logical time state for processing requests. That is, before each operation starts, the local logical time must be updated with the logical time from the log. Therefore, all write operations are deterministic. In effect, advancing the logical time is attached to each entry, and the actual overhead of this operation is negligible — it is just modifying a local integer value.

**The expiration time of the core cache Map is changed from relying on the physical clock to relying on the write logical clock.**

Obviously, for write operations, both the primary and replica nodes directly process the request using the logical timestamp. All lazy deletions and timed deletions must be performed deterministically based on the logical time in the state machine. The consistency of write operations is easy to prove: the state machine iteration is performed deterministically through logs.

Read requests are the focus of this algorithm's optimization. Reads also need to generate a read logical timestamp. This timestamp similarly takes `max(write_logical_time, read_logical_time, physical_time)`. Unlike write timestamps, read timestamps do not need to go through consensus to be updated. They can be maintained directly in memory and are updated with every read. Obviously, this read timestamp is monotonically increasing on a single node (no rollback issues). And because the clock offset is bounded and the leader election time > clock offset time, when a new primary node is elected, its local physical time must be greater than the previous primary node's local physical time. Therefore, globally, the logical time generated by read requests is monotonically increasing.

A read logical time will not cause a pair to be deleted from memory, but if `expiration_time < read_logical_timestamp`, the pair is considered nonexistent.

Primary node handling a read request: If the primary node reads a record that has been deleted, even if the primary node fails and a new primary is elected, reading from the new primary will still show that the record has been deleted (because the time has passed, and the clock offset is bounded).

However, replica nodes cannot perform scheduled deletion operations on their own. When no write requests come in, the primary and replica nodes cannot perform GC. We can submit an empty log once per minute to manually trigger a change in the write logical timestamps of both the primary and replica nodes, thereby triggering GC. This avoids high-frequency log submissions when there are no external requests, and the GC efficiency gap between the primary and replica nodes is only about 10 seconds. Furthermore, if no keys are deleted, there is no need to send messages to the replica nodes.

> If the primary and replica nodes do not advance the logical timestamp via Raft (e.g., using heartbeats), it is possible that a replica node, after being unable to contact the primary for an extended period, deletes expired entries. Then, if the primary generates a set nx command that executes on the primary before expiration, but the command is sent to the replica only after the replica has already deleted the key, ambiguity in the execution results between the replica and the primary may occur.

Issue: Additionally, this solution requires something like Google's TrueTime API to ensure that the absolute time offset between nodes is within an acceptable range. However, since we can tolerate offsets of several hundred milliseconds, NTP time synchronization is usually sufficient.

## Algorithm Proof

### **Enumeration of All Read Scenarios**

> Ambiguity arises when, during a primary-replica switchover, the local times on both sides cannot reach consensus. However, after the switchover, the local clock of the new primary node must be greater than the local clock of the old primary node before it went down (leader election time > time offset). The clock continues to move forward. Therefore, the semantics of read operations exposed externally are always moving forward in time.

There are three possible outcomes when reading from the primary node:

1. No data found (no return): This is fine because the data either never existed or has been deterministically deleted by the progression of time caused by other write operations (this deletion has already achieved consensus).
2. Data found but expired (no return)
3. Data found and not expired (return)

***

Case 2 is divided into two sub-cases:

- Both the primary and replica have deleted the data: This is fine; they are consistent.
- The primary has deleted the data, but the replica has not yet deleted it. Because the clock offset between the primary and replica is bounded, if no primary-replica switchover occurs, there is no issue. However, if a switchover occurs, since the election time is greater than the clock offset between nodes, after the new primary is elected, a read operation will also return that the data is expired.

***

Case 3 is divided into two sub-cases:

- The replica expires slightly later than the primary. If the primary goes down and a new primary is elected, reading from the new primary may find the data expired. This case does not cause a problem because the primary itself was also about to expire. The difference in expiration times between nodes due to clock offset is acceptable.
- The replica expires slightly earlier than the primary. In this case, the primary goes down, a new primary is elected, and the new primary will return the value normally. So there is no problem.

### Detailed Proof

Goal: Once an operation determines that a key has expired, all subsequent operations will treat that key as expired.

When a pair that needs to expire is set, it stores an absolute point in time as its expiration time.

Operations that modify the state machine state are collectively referred to as write operations. Many write operations are RMW (read-modify-write) and also require reading. For example, Redis's append command needs to first read the current pair before modifying its value.

For write operations: If `expiration_time < write_logical_timestamp`, it is considered expired. This write logical time is committed to the state machine via Raft. It is generated as `max(write_logical_time, read_logical_time, physical_time)`.

For read operations: If `expiration_time < read_logical_timestamp`, it is considered expired (without initiating additional consensus). The logical time for read operations is maintained in memory. It is generated as `max(write_logical_time, read_logical_time, physical_time)`.

The logical time for write operations is obviously monotonically increasing, because each change produced by a write operation is committed to Raft and only takes effect after the full consensus process is completed.

On a single machine, read operations are obviously monotonically increasing. When a node switchover occurs, since cache-cat requires the primary-replica switchover time to be greater than the clock offset between all nodes (achievable by configuring the Raft election timeout, typically not exceeding 1 second), the logical time for read operations remains monotonically increasing even during node switchovers: when the next primary node is elected, its local physical time must be greater than the physical time of the old primary node.

> Some details:
> The read logical clock and the write logical clock are concurrent. This means that when a read logical clock is generated, another thread may generate a larger write logical clock. This could cause data that would have been accessible by the read logical clock to not be found. However, this has no real impact, because in a concurrent scenario, we do not require ordering between reads and writes; i.e., whichever gets the larger sequence number is acceptable.
>
> Taking the write operation clock as an example. The write operation clock is generated asynchronously and then pushed into a queue. Therefore, it is possible that a log with a larger write clock is actually placed before one with a smaller clock. This is allowed. We always compute `max(write_logical_time, read_logical_time, physical_time)`, so this timestamp is monotonically increasing. If a similar problem occurs, all nodes will perform the same operation: the write logical clock will not be updated. Unlike HLC, our write logical clock here does not need to be unique; it is only used to ensure consistency among all nodes.

We can easily deduce:

- Write requests received after a write request will have a logical time greater than the previous write request. (This is guaranteed by Raft state machine iteration.)
- Write requests received after a read request returns will have a logical time greater than the read request.
- Similarly, read requests received after a write request returns will have a logical time greater than the write request.
- Similarly, read requests received after a read request returns will have a logical time greater than the previous read request.

Obviously:

- When a write operation returns that data has expired, subsequent write operations will also determine that the data has expired.
- When a read operation returns that data has expired, subsequent write operations will also determine that the data has expired.
- When a write operation determines that data has expired, subsequent read operations will also determine that the data has expired.
- When a read operation returns that data has expired, subsequent read operations will also determine that the data has expired.
