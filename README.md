# Cache-Cat

<div align="center">
<a href="https://github.com/nasuiyile/cache-cat/blob/master/README.md">English</a> ｜
<a href="https://github.com/nasuiyile/cache-cat/blob/master/README_zh.md">简体中文</a> |
<a href="https://nasuiyile.github.io/cache-cat-website">Official Website</a> 
</div>

## Introduction

Cache-Cat is a high-performance key-value cache library that leverages the Raft consensus protocol to provide both high availability and strong consistency.

The goal of Cache-Cat is to build an extremely high-performance cache framework with disaster recovery capabilities based on Raft. Unlike traditional cache systems such as Redis and Memcached, Cache-Cat is designed to ensure that committed data is never lost.

In terms of positioning, the most comparable project is [RedisRaft](https://github.com/RedisLabs/redisraft?utm_source=chatgpt.com), which enables Redis instances to form a strongly consistent Raft cluster. However, RedisRaft is primarily a laboratory and research project.

> Even when Redis cluster solutions are used, Redis can still lose data. Redis clustering primarily addresses availability rather than data consistency.

Compared with systems such as etcd, Apache ZooKeeper, Consul, and TiKV, which also rely on consensus algorithms and provide reliable data storage, Cache-Cat offers significantly lower latency and higher throughput. Under the same environment and default configurations, Cache-Cat can achieve approximately 500,000 writes per second, whereas TiKV achieves around 200,000 writes per second.

Furthermore, these systems were not originally designed as caching solutions and therefore lack many cache-oriented features such as:

- LRU (Least Recently Used) eviction
- LFU (Least Frequently Used) eviction
- Maximum memory usage limits

## Features

For many small and medium-sized companies, introducing numerous infrastructure components can increase operational complexity. Cache-Cat aims to provide a unified solution for multiple common use cases.

In theory, Cache-Cat can be used as:

### Key-Value Database

Similar to TiKV, Cache-Cat can store critical business data that must not be lost.

### Service Discovery and Configuration Center

Similar to Consul and Apache ZooKeeper, Cache-Cat can serve as a service registry and configuration management platform, provided suitable client integrations are implemented.

### Distributed Cache

Similar to Redis, Dragonfly, and Valkey.

Caching workloads are predominantly read-heavy. At the read layer, Cache-Cat delivers performance comparable to modern in-memory caches. For write operations, Cache-Cat still provides over **150,000 operations per second**.

According to industry surveys, most production systems exhibit read-to-write ratios around **95:5**, and caches themselves cannot fundamentally accelerate database writes. Therefore, Cache-Cat's write throughput is sufficient for the vast majority of applications.

### Distributed Locking

Cache-Cat can also be used for distributed locking, similar to ZooKeeper and etcd.

Compared with Redis-based distributed locks, Cache-Cat provides stronger consistency guarantees. Redis distributed locking solutions have well-known limitations and corner cases, even when using algorithms such as Redlock.

For additional discussion, see:

[Is Redlock safe?](https://antirez.com/news/101?utm_source=chatgpt.com)

Comprehensive benchmark results will be published once the project reaches feature completeness.

------

## Consistency Model

A frequently discussed topic is the consistency between a cache and a database during dual-write operations.

For example:

[Ctrip's Strong and Eventual Consistency Cache Practice](https://mp.weixin.qq.com/s/E-chAZyHtaZOdA19mW59-Q?utm_source=chatgpt.com)

However, cache-database consistency is a different problem from the consistency model of a database itself.

The consistency discussed in this document refers to the database's internal consistency guarantees, not cache-database synchronization strategies.

In simple terms:

- **Strong Consistency**: Once a write operation succeeds, all subsequent reads immediately observe the latest value. Externally, the system behaves as a single deterministic state machine.
- **Eventual Consistency**: Updates may require some time to propagate before all readers observe the latest value.

Cache-Cat provides **strong consistency** through the Raft consensus protocol.

------

# FAQ

## What is the difference between Cache-Cat and TiKV?

**Answer:**

TiKV is a distributed database that uses Raft for replication, with persistent storage engines underneath. Data is designed to be stored durably on disk.

Cache-Cat takes a different approach. To maximize performance, the state machine data itself resides entirely in memory and is not persisted as a database storage layer.

As a result:

- TiKV prioritizes durable persistent storage.
- Cache-Cat prioritizes ultra-fast in-memory access while maintaining strong consistency through replicated Raft logs.

------

## Can Redis really lose data, even when running in a cluster?

**Answer:**

Yes.

Neither Redis Cluster nor Redis Sentinel relies on a consensus protocol.

In a typical Redis replication setup:

1. The primary node processes a write request.
2. The primary replies to the client.
3. The primary asynchronously replicates the update to replicas.

If the primary crashes after acknowledging the write but before replication completes, and a replica is promoted to become the new primary, the acknowledged write can be permanently lost.

Redis clusters may also experience split-brain scenarios under certain failure conditions.

------

## Doesn't Raft require disk writes and replication before responding, making it unsuitable for a cache?

**Answer:**

Not exactly.

Raft requires **log entries** to be persisted and replicated according to the protocol, but it does not require the state machine data itself to be stored on disk.

In Cache-Cat:

- Raft logs are persisted for durability.
- State machine data is stored entirely in memory using structures such as hash maps and other optimized in-memory data structures.

A useful comparison is Redis AOF:

- The persisted Raft log is analogous to Redis AOF logs.
- The actual data structures remain memory-resident.

Several widely used Raft-based systems follow a similar design philosophy:

- ZooKeeper stores the entire ZNode tree in memory.
- Consul stores its key-value data in memory.
- etcd, by contrast, persists state machine data to disk.

For caching workloads, replication and log persistence inevitably increase write latency compared to a pure in-memory cache. This tradeoff is unavoidable.

However:

- Read operations require no additional disk access.
- Read performance remains extremely fast.
- The cost is slightly higher write latency.
- The benefit is that committed data is not lost.

We believe that cache systems are typically more sensitive to read latency than write latency, making this tradeoff worthwhile.

------

## License

This project includes code derived from:

- [coredb](https://github.com/lichuang/coredb?utm_source=chatgpt.com)
- [rockraft](https://github.com/lichuang/rockraft?utm_source=chatgpt.com)

These components are licensed under the **Apache License 2.0**.