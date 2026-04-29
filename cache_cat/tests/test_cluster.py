from time import sleep

import redis
import time

r = redis.Redis(
    # db=0,
    host='localhost',
    port=6379,
    decode_responses=True
)

# 设置 key，1 秒后过期
r.set('name', "hello")
r.append('name'," world!")
print(r.get('name'))
