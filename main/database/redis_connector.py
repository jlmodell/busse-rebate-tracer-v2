import redis
from constants import REDIS_PASS, REDIS_PORT, REDIS_URL

rdb = redis.Redis(host=REDIS_URL, port=int(REDIS_PORT), password=REDIS_PASS, db=0)
