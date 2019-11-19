from . import sync_logging, sync_irods
from .sync_utils import app, cleanup_key, count_key, dequeue_key,
    done, failures_key, get_redis,
    get_with_key, reset_with_key, retries_key,
    set_with_key, stop_key, tasks_key
from .sync_task import cleanup, periodic, restart
from os.path import realpath
from uuid import uuid1
import json
import progressbar
import redis_lock
import time

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

def list_synchronization(config):
    r = get_redis(config)
    with redis_lock.Lock(r, "lock:periodic"):
        return {"periodic":list(map(lambda job_id: job_id.decode("utf-8"), r.lrange("periodic", 0, -1))),
                "singlepass":list(map(lambda job_id: job_id.decode("utf-8"), r.lrange("singlepass", 0, -1)))}

def monitor_synchronization(job_name, progress, config):
    logger = sync_logging.get_sync_logger(config["log"])

    r = get_redis(config)
    if get_with_key(r, cleanup_key, job_name, str) is None:
        logger.error("job [{0}] does not exist".format(job_name))
        raise Exception("job [{0}] does not exist".format(job_name))

    if progress:
        widgets = [
            ' [', progressbar.Timer(), '] ',
            progressbar.Bar(),
            ' (', progressbar.ETA(), ') ',
            progressbar.DynamicMessage("count"), " ",
            progressbar.DynamicMessage("tasks"), " ",
            progressbar.DynamicMessage("failures"), " ",
            progressbar.DynamicMessage("retries")
        ]

        with progressbar.ProgressBar(max_value=1, widgets=widgets, redirect_stdout=True, redirect_stderr=True) as bar:
            def update_pbar():
                total2 = get_with_key(r, tasks_key, job_name, int)
                total = r.llen(count_key(job_name))
                if total == 0:
                    percentage = 0
                else:
                    percentage = max(0, min(1, (total - total2) / total))

                failures = get_with_key(r, failures_key, job_name, int)
                retries = get_with_key(r, retries_key, job_name, int)

                bar.update(percentage, count=total, tasks=total2, failures=failures, retries=retries)

            while not done(r, job_name) or periodic(r, job_name):
                update_pbar()
                time.sleep(1)

            update_pbar()

    else:
        while not done(r, job_name) or periodic(r, job_name):
            time.sleep(1)

    failures = get_with_key(r, failures_key, job_name, int)
    if failures != 0:
        return -1
    else:
        return 0

def start_synchronization(data):
    config = data["config"]
    logging_config = config["log"]
    root = data["root"]
    job_name = data["job_name"]
    interval = data["interval"]
    restart_queue = data["restart_queue"]
    sychronous = data["synchronous"]
    progress = data["progress"]
    s3_region_name = data["s3_region_name"]
    s3_endpoint_domain = data["s3_endpoint_domain"]
    s3_keypair = data["s3_keypair"]

    logger = sync_logging.get_sync_logger(logging_config)
    data_copy = data.copy()

    if s3_keypair is not None:
        data_copy['s3_region_name'] = s3_region_name
        data_copy['s3_endpoint_domain'] = s3_endpoint_domain
        data_copy['s3_keypair'] = s3_keypair
        # parse s3 keypair
        if s3_keypair is not None:
            with open(s3_keypair) as f:
                data_copy['s3_access_key'] = f.readline().rstrip()
                data_copy['s3_secret_key'] = f.readline().rstrip()
        # set root
        root_abs = root
    else:
        root_abs = realpath(root)

    data_copy["root"] = root_abs
    data_copy["path"] = root_abs

    sync_irods.validate_target_collection(data_copy, logger)

    def store_event_handler(data):
        event_handler = data.get("event_handler")
        event_handler_data = data.get("event_handler_data")
        event_handler_path = data.get("event_handler_path")

        if event_handler is None and event_handler_path is not None and event_handler_data is not None:
            event_handler = "event_handler" + uuid1().hex
            hdlr2 = event_handler_path + "/" + event_handler + ".py"
            with open(hdlr2, "w") as f:
                f.write(event_handler_data)
            cleanup_list = [hdlr2.encode("utf-8")]
            data["event_handler"] = event_handler
        else:
            cleanup_list = []
        set_with_key(r, cleanup_key, job_name, json.dumps(cleanup_list))

    r = get_redis(config)
    with redis_lock.Lock(r, "lock:periodic"):
        if get_with_key(r, cleanup_key, job_name, str) is not None:
            logger.error("job {0} already exists".format(job_name))
            raise Exception("job {0} already exists".format(job_name))

        store_event_handler(data_copy)

    if interval is not None:
        r.rpush("periodic", job_name.encode("utf-8"))

        restart.s(data_copy).apply_async(queue=restart_queue, task_id=job_name)
    else:
        r.rpush("singlepass", job_name.encode("utf-8"))
        if not sychronous:
            restart.s(data_copy).apply_async(queue=restart_queue)
        else:
            res = restart.s(data_copy).apply()
            if res.failed():
                print(res.traceback)
                cleanup(r, job_name)
                return -1
            else:
                return monitor_synchronization(job_name, progress, config)

