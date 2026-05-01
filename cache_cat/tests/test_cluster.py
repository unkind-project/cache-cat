from time import sleep

import redis
import time

r = redis.Redis(
    # db=0,
    host='localhost',
    port=6379,
    decode_responses=True
)

r.lpush('32112', '2')
# 设置 key，1 秒后过期
print(r.lrange('32112', 0, -1))