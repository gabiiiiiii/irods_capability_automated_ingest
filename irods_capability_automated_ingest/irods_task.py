from . import sync_logging, sync_irods
from .custom_event_handler import custom_event_handler
from .sync_job import sync_job
from .sync_utils import app
from .utils import enqueue_task
from .task_queue import task_queue
from .scanner import scanner_factory
import traceback
from celery.signals import task_prerun, task_postrun
from billiard import current_process

class IrodsTask(app.Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        meta = args[0]
        config = meta["config"]
        job = sync_job.from_meta(meta)
        logger = sync_logging.get_sync_logger(config["log"])
        logger.error('failed_task', task=meta["task"], path=meta["path"], job_name=job.name(), task_id=task_id, exc=exc, einfo=einfo, traceback=traceback.extract_tb(exc.__traceback__))
        job.failures_handle().incr()

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        meta = args[0]
        config = meta["config"]
        job = sync_job.from_meta(meta)
        logger = sync_logging.get_sync_logger(config["log"])
        logger.warning('retry_task', task=meta["task"], path=meta["path"], job_name=job.name(), task_id=task_id, exc=exc, einfo=einfo, traceback=traceback.extract_tb(exc.__traceback__))
        job.retries_handle().incr()

    def on_success(self, retval, task_id, args, kwargs):
        meta = args[0]
        config = meta["config"]
        logger = sync_logging.get_sync_logger(config["log"])
        job_name = meta["job_name"]
        logger.info('succeeded_task', task=meta["task"], path=meta["path"], job_name=job_name, task_id=task_id, retval=retval)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        meta = args[0]
        config = meta["config"]
        job = sync_job.from_meta(meta)
        logger = sync_logging.get_sync_logger(config["log"])
        logger.info('decr_job_name', task=meta["task"], path=meta["path"], job_name=job.name(), task_id=task_id, retval=retval)

        done = job.tasks_handle().decr() and not job.periodic()
        if done:
            job.cleanup()

        job.dequeue_handle().rpush(task_id)

        if done:
            event_handler = custom_event_handler(meta)
            if event_handler.hasattr('post_job'):
                module = event_handler.get_module()
                module.post_job(module, logger, meta)


@app.task(bind=True, base=IrodsTask)
def sync_path(self, meta):
    syncer = scanner_factory(meta)
    syncer.sync_path(self, meta)

@app.task(bind=True, base=IrodsTask)
def sync_dir(self, meta):
    syncer = scanner_factory(meta)
    syncer.sync_entry(self, meta, "dir", sync_irods.sync_data_from_dir, sync_irods.sync_metadata_from_dir)

@app.task(bind=True, base=IrodsTask)
def sync_files(self, _meta):
    chunk = _meta["chunk"]
    meta = _meta.copy()
    for path, obj_stats in chunk.items():
        meta['path'] = path
        meta["is_empty_dir"] = obj_stats.get('is_empty_dir')
        meta["is_link"] = obj_stats.get('is_link')
        meta["is_socket"] = obj_stats.get('is_socket')
        meta["mtime"] = obj_stats.get('mtime')
        meta["ctime"] = obj_stats.get('ctime')
        meta["size"] = obj_stats.get('size')
        meta['task'] = 'sync_file'
        syncer = scanner_factory(meta)
        syncer.sync_entry(self, meta, "file", sync_irods.sync_data_from_file, sync_irods.sync_metadata_from_file)

