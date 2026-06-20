# Deletion Policy

> All of the following is based on secondary development on top of moka

## LRU and TinyLfu Deletion

Limitations of the Raft algorithm itself: For performance reasons, read operations do not go through Raft consensus and can run concurrently with Raft consensus, which may lead to inaccurate deletion of data.

When you access data that is already in the process of being deleted, you may still be able to read this data, but the data will eventually be deleted, although the probability of this happening is very low.

Both LRU and TinyLfu deletions will be initiated by the primary node through consensus to ensure determinism in data iteration.

We forked moka's source code to obtain the data at the end of the LRU and LFU queues — i.e., the least frequently accessed data. A consensus is then initiated to delete this data. It is possible that before the deletion consensus is completed, this data is accessed again; in this case, the data will still be deleted.

## Scheduled Deletion

Cache-cat itself uses flurry-map to implement a multi-threaded concurrent Map.
