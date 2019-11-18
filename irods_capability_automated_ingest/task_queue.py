from . import sync_logging
from .sync_job import sync_job
from .custom_event_handler import custom_event_handler
from uuid import uuid1

class task_queue(object):
    def __init__(self, name, meta):
        self.queue_name = name
        self.meta = meta.copy()

    def name(self):
        return self.queue_name

    def get_scanner_type(self):
        if self.meta.get('s3_keypair'):
            return 's3_scanner'
        return 'filesystem_scanner'


    def add(self, task):
        logger = sync_logging.get_sync_logger(self.meta["config"]["log"])
        job = sync_job.from_meta(self.meta)
        if job.is_stopping():
            # A job by this name is currently being stopped
            logger.info('async_job_name_stopping', task=self.meta["task"], path=self.meta["path"], job_name=job.name())
            pass

        logger.info('incr_job_name', task=self.meta["task"], path=self.meta["path"], job_name=job.name())
        job.tasks_handle().incr()
        task_id = str(uuid1())
        timeout = custom_event_handler(self.meta).timeout()
        job.count_handle().rpush(task_id)
        scanner_type = self.get_scanner_type()
        task.s(self.meta, scanner_type).apply_async(queue=self.name(), task_id=task_id, soft_time_limit=timeout)

