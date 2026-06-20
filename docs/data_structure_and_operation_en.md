# Data Structure Design

## Data Structure Documentation

> For simplicity, Redis's basic data structures are used as headings. Most operations are still in the development adaptation phase.

### String

Currently implemented via moka. All key-value operations for the following data structures use moka as the foundation.

### Hash

Implemented using Rust's built-in hashmap. For concurrent read and write, only flurry can be used.

### List

Implemented using `std::collections::LinkedList`. For concurrent read and write, crossbeam's SegQueue can be used.

### Set

An unordered set, currently implemented using Rust's hashset. For concurrent read and write, only flurry can be used.

### Sorted Set

Also called Zset in Redis. Currently implemented using Rust's built-in BTreeSet instead of crossbeam's skiplist.

### Others

It is known that Redis also supports bitmap, hyperloglog, Geospatial, Streams, Bitfield, etc. There are currently no plans to support them.

## Read-Write Concurrency Model

In Redis, there is a core execution queue, the dispatcher, and all commands are executed in a single thread. However, this is not suitable in Raft, as there is a significant latency gap between read and write commands. Read commands can typically return directly within the lease period, while write commands must go through a full round of the consensus algorithm. Additionally, the core Raft execution is also single-threaded.
Naturally, cache-cat uses a read-write concurrent Hashmap.

Rust's moka library ensures that each single-key operation is atomic. Read-read conflicts obviously do not occur. Write-write operations are serialized, so atomicity issues do not arise. (Write here includes delete operations)

|                 | Single-key Write (multiple) | Multi-key Write | Single-key Read | Multi-key Read |
| --------------- | --------------------------- | --------------- | --------------- | -------------- |
| Single-key Read (multiple)  | ✓                           | x               | ✓               | ✓              |
| Multi-key Read              | x                           | x               | ✓               | ✓              |

When a multi-key read encounters multiple single-key writes — for example, reading k1 and k2 via mget: read k1 -> write k1 -> write k2 -> read k2 — atomicity issues arise. The same applies to multi-key writes.

When a single-key read encounters a multi-key write: write k1 -> read k1 -> read k2 -> write k2 — an intermediate state is read.

Cache-cat uses read_lock and write_lock to optimize this issue.

|                 | read_lock (RwLock) | write_lock (Mutex) |
| --------------- | ------------------ | ------------------ |
| Single-key Read (multiple)  | Read lock          | x                  |
| Multi-key Read              | Read lock          | ✓                  |
| Single-key Write (multiple) | x                  | ✓                  |
| Multi-key Write             | Write lock         | ✓                  |

Since multi-key reads and writes are absolute minority operations, both locks only need to be acquired for multi-key operations.
