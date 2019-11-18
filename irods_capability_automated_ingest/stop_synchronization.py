from . import sync_logging
from .sync_utils import app, get_redis, cleanup_key, get_with_key, set_with_key, stop_key, reset_with_key, dequeue_key, count_key
from .sync_task import cleanup
import progressbar
import redis_lock

def interrupt(r, job_name, cli=True, terminate=True):
    set_with_key(r, stop_key, job_name, "")
    tasks = list(map(lambda x: x.decode("utf-8"), r.lrange(count_key(job_name), 0, -1)))
    tasks2 = set(map(lambda x: x.decode("utf-8"), r.lrange(dequeue_key(job_name), 0, -1)))

    tasks = [item for item in tasks if item not in tasks2]

    if cli:
        tasks = progressbar.progressbar(tasks, max_value=len(tasks))

    # stop active tasks for this job
    for task in tasks:
        app.control.revoke(task, terminate=terminate)

    # stop restart job
    app.control.revoke(job_name)

    reset_with_key(r, stop_key, job_name)


def stop_synchronization(job_name, config):
    logger = sync_logging.get_sync_logger(config["log"])

    r = get_redis(config)
    with redis_lock.Lock(r, "lock:periodic"):
        if get_with_key(r, cleanup_key, job_name, str) is None:
            logger.error("job [{0}] does not exist".format(job_name))
            raise Exception("job [{0}] does not exist".format(job_name))
        else:
            interrupt(r, job_name)
            cleanup(r, job_name)

