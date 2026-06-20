# Cluster Access Strategy

## Client Connection Strategy

> The "client" mentioned below refers to any client that needs to connect to cache-cat. This document is currently a conceptual draft and is not yet fully implemented.

There are theoretically multiple approaches using external solutions to achieve high availability.
For example, using a proxy to forward requests and handle primary-replica failover. Stateless proxies can proactively communicate with all nodes in the cluster to verify liveness.
Alternatively, Kubernetes can be used. Although K8s itself does not know which node is the primary or replica, this can be achieved through an operator solution: https://github.com/dragonflydb/dragonfly-operator.
However, all the above solutions are too complex for cache-cat. Cache-cat chooses to directly implement commands compatible with Redis Sentinel clusters, configuring all nodes within the cluster as sentinel nodes. In cache-cat, both primary and replica nodes can respond to basic sentinel-related commands.

In the original Redis Sentinel cluster, the Redis service and the Redis Sentinel service are actually two separate processes, occupying ports 6379 and 26379 respectively. Configuration is quite complex.
However, due to semantic differences in the cluster, not all fields have direct one-to-one mappings. To reduce the complexity of sentinel configuration, cache-cat parses sentinel commands directly on the default Redis port.

Python code example:
~~~python
from time import sleep

from redis.sentinel import Sentinel
from redis.retry import Retry
from redis.backoff import ExponentialBackoff
sentinel = Sentinel(
    [
        ('127.0.0.1', 6379),
        ('127.0.0.1', 6380),
        ('127.0.0.1', 6381),
    ],
    socket_timeout=0.5,
    socket_connect_timeout=0.5,
)
master = sentinel.master_for(
    'cat',
    db=0,
    socket_timeout=0.5,
    socket_connect_timeout=0.5,
    retry=Retry(
        ExponentialBackoff(base=0.1, cap=1),
        retries=5,
    )
)
master.set('foo', 'bar')
while True:
    sleep(1)
    print(master.get('foo'))
~~~

The corresponding configuration files are located in the `./cache-cat/conf` directory. We still recommend that you implement thorough error handling logic. In cache-cat, replica nodes cannot process any requests. If a request is sent to a replica node, it will directly return a read error. Some clients may default to sending read requests to replica nodes; this needs to be corrected in the configuration.

***
Why doesn't Cache-cat support reading data from replica nodes?
Because of the limitations of the Redis protocol, if replica nodes are allowed to handle read requests, when a primary-replica failover occurs, read requests may be sent to a replica node, and the returned data may not be the latest.
ETCD solves this problem by adding an additional flag to requests. Therefore, when allowing reads from replicas, strong consistent reads cannot be achieved under the semantics of the Redis protocol.
