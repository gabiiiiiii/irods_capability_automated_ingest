from .sync_utils import get_redis
import redis_lock

def list_synchronization(config):
    r = get_redis(config)
    with redis_lock.Lock(r, "lock:periodic"):
        return {"periodic":list(map(lambda job_id: job_id.decode("utf-8"), r.lrange("periodic", 0, -1))),
                "singlepass":list(map(lambda job_id: job_id.decode("utf-8"), r.lrange("singlepass", 0, -1)))}
