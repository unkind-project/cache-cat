from time import sleep

import redis
import time

r = redis.Redis(
    # db=0,
    host='localhost',
    port=6379,
    decode_responses=True
)

r.hset('321', 'name', '123')
print()
# 设置 key，1 秒后过期
print(r.hget('321', 'nam1e'))
