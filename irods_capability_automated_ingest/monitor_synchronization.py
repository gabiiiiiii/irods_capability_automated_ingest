from . import sync_logging
from .sync_utils import get_redis, get_with_key, done, count_key, cleanup_key, tasks_key, failures_key, retries_key
from .sync_task import periodic
import progressbar
import time

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

