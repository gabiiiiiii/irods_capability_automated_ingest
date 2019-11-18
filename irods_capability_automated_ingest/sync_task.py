from . import sync_logging, sync_irods
from .custom_event_handler import custom_event_handler
from .sync_job import sync_job
from .sync_utils import app
from .task_queue import task_queue
import traceback
from celery.signals import task_prerun, task_postrun
from billiard import current_process
import irods_capability_automated_ingest.irods_task

@task_prerun.connect()
def task_prerun(task_id=None, task=None, args=None, kwargs=None, **kw):
    meta = args[0]
    if meta["profile"]:
        config = meta["config"]
        profile_log = config.get("profile")
        logger = sync_logging.get_sync_logger(profile_log)
        logger.info("task_prerun", event_id=task_id, event_name=task.name, path=meta.get("path"), target=meta.get("target"), hostname=task.request.hostname, index=current_process().index)

@task_postrun.connect()
def task_postrun(task_id=None, task=None, args=None, kwargs=None, retval=None, state=None, **kw):
    meta = args[0]
    if meta["profile"]:
        config = meta["config"]
        profile_log = config.get("profile")
        logger = sync_logging.get_sync_logger(profile_log)
        logger.info("task_postrun", event_id=task_id, event_name=task.name, path=meta.get("path"), target=meta.get("target"), hostname=task.request.hostname, index=current_process().index,state=state)


class RestartTask(app.Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        meta = args[0]
        config = meta["config"]
        job_name = meta["job_name"]
        logger = sync_logging.get_sync_logger(config["log"])
        logger.error('failed_restart', path=meta["path"], job_name=job_name, task_id=task_id, exc=exc, einfo=einfo, traceback=traceback.extract_tb(exc.__traceback__))

@app.task(base=RestartTask)
def restart(meta):
    # Start periodic job on restart_queue
    job_name = meta["job_name"]
    restart_queue = meta["restart_queue"]
    interval = meta["interval"]
    if interval is not None:
        restart.s(meta).apply_async(task_id=job_name, queue=restart_queue, countdown=interval)

    # Continue with singlepass job
    config = meta["config"]
    logging_config = config["log"]
    logger = sync_logging.get_sync_logger(logging_config)
    try:
        event_handler = custom_event_handler(meta)
        if event_handler.hasattr('pre_job'):
            module = event_handler.get_module()
            module.pre_job(module, logger, meta)

        logger.info("***************** restart *****************")
        job = sync_job.from_meta(meta)
        if not job.periodic() or job.done():
            logger.info("no tasks for this job and worker handling this task is not busy")

            job.reset()
            meta = meta.copy()
            meta["task"] = "sync_path"
            meta['queue_name'] = meta["path_queue"]
            task_queue(meta["path_queue"], meta).add(irods_capability_automated_ingest.irods_task.sync_path)
        else:
            logger.info("tasks exist for this job or worker handling this task is busy")

    except OSError as err:
        logger.warning("Warning: " + str(err), traceback=traceback.extract_tb(err.__traceback__))

    except Exception as err:
        logger.error("Unexpected error: " + str(err), traceback=traceback.extract_tb(err.__traceback__))
        raise
