
import redis
direct_sentinel_conn = redis.Redis(host='127.0.0.1', port=6380)
masters_info = direct_sentinel_conn.sentinel_masters()

print(masters_info)