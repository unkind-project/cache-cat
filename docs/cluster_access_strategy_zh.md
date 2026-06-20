# 集群访问策略

## 客户端连接策略

> 下文的客户端指的都是需要连接cache-cat的客户端。该文档只是作为构想，尚未完全实现。

理论上存在多种通过外部方案结合实现高可用的方案。
如通过proxy来代理转发进行主从切换。无状态的proxy可以主动和集群内的全部节点通信来确认是否存活。
或者通过Kubernetes，虽然k8s本身不知道谁是primary谁是replica但可以通过operator方案来实现：https://github.com/dragonflydb/dragonfly-operator。
但是以上方案对于cache-cat来说均过于复杂。cache-cat选择直接兼容Redis的哨兵集群相关的命令。直接将集群内的所有节点配置为哨兵节点。在cache-cat中主节点和从节点都可以响应哨兵相关的基础命令。
在原本的Redis哨兵集群中，Redis服务和Redis哨兵服务事实上是俩个进程，分别占用6379端口和26379端口。配置起来相当复杂。
然而由于集群的语义不一致，并不是所有字段都有一一对应。为了减少哨兵配置的复杂度。cache-cat直接在默认配置的Redis端口中来解析哨兵命令。
python代码如下：
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
对应的配置文件在./cache-cat/conf目录下。我们仍然建议你定义完善的错误处理逻辑。因为在cache-cat中从节点不能处理任何请求。此时从节点会直接返回读取错误。部分客户端，可能允许读请求默认走从节点，需要在配置中进行修正。
***
为什么Cache-cat不支持从节点数据读取？
因为Redis协议的限制，如果从节点被允许处理读请求，那么当主从发生切换时，读取请求可能被发送到从节点。此时返回的并不是最新的数据。
ETCD通过在请求中额外增加标志位来解决该问题。因此当允许从节点读取时，在Redis协议的语义下就无法实现强一致读。