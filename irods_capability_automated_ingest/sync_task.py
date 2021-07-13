from . import sync_logging, sync_irods
from .custom_event_handler import custom_event_handler
from .sync_job import sync_job
from .sync_utils import app
from .utils import enqueue_task
from .task_queue import task_queue
#from .scanner import scanner_factory
import traceback
from celery.signals import task_prerun, task_postrun
from billiard import current_process

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
            #task_queue(meta["path_queue"]).add(sync_path, meta)
            meta['queue_name'] = meta["path_queue"]
            enqueue_task(sync_path, meta)
        else:
            logger.info("tasks exist for this job or worker handling this task is busy")

    except OSError as err:
        logger.warning("Warning: " + str(err), traceback=traceback.extract_tb(err.__traceback__))

    except Exception as err:
        logger.error("Unexpected error: " + str(err), traceback=traceback.extract_tb(err.__traceback__))
        raise

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

# Use the built-in version of scandir/walk if possible, otherwise
# use the scandir module version
try:
    from os import scandir
except ImportError:
    from scandir import scandir

import stat
import os
from os.path import join, getmtime, relpath, getctime
from datetime import datetime
import redis_lock
#from . import sync_logging
#from .custom_event_handler import custom_event_handler
from .redis_key import sync_time_key_handle
from .sync_utils import get_redis
#from .utils import enqueue_task
#from .task_queue import task_queue
from minio import Minio
#from billiard import current_process
import base64
import re


class scanner(object):
    def __init__(self, meta):
        self.meta = meta.copy()

    def exclude_file_type(self, dir_regex, file_regex, full_path, logger, mode=None):
        ex_list = self.meta['exclude_file_type']
        if len(ex_list) <= 0 and None == dir_regex and None == file_regex:
            return False

        ret_val = False
        mode    = None

        dir_match = None
        for d in dir_regex:
            dir_match = None != d.match(full_path)
            if(dir_match == True):
                break

        file_match = None
        for f in file_regex:
            file_match = None != f.match(full_path)
            if(file_match == True):
                break

        try:
            if mode is None:
                mode = os.lstat(full_path).st_mode
        except FileNotFoundError:
            return False

        if stat.S_ISREG(mode):
            if 'regular' in ex_list or file_match:
                ret_val = True
        elif stat.S_ISDIR(mode):
            if 'directory' in ex_list or dir_match:
                ret_val = True
        elif stat.S_ISCHR(mode):
            if 'character' in ex_list or file_match:
                ret_val = True
        elif stat.S_ISBLK(mode):
            if 'block' in ex_list or file_match:
                ret_val = True
        elif stat.S_ISSOCK(mode):
            if 'socket' in ex_list or file_match:
                ret_val = True
        elif stat.S_ISFIFO(mode):
            if 'pipe' in ex_list or file_match:
                ret_val = True
        elif stat.S_ISLNK(mode):
            if 'link' in ex_list or file_match:
                ret_val = True

        return ret_val

class filesystem_scanner(scanner):
    def __init__(self, meta):
        super(filesystem_scanner, self).__init__(meta)

    def sync_path(self, task_cls, meta):
        task = meta["task"]
        path = meta["path"]
        path_q_name = meta["path_queue"]
        file_q_name = meta["file_queue"]
        config = meta["config"]
        logging_config = config["log"]
        exclude_file_name = meta['exclude_file_name']
        exclude_directory_name = meta['exclude_directory_name']

        logger = sync_logging.get_sync_logger(logging_config)

        file_regex = [ re.compile(r) for r in exclude_file_name ]
        dir_regex  = [ re.compile(r) for r in exclude_directory_name ]

        try:
            logger.info("walk dir", path=path)
            meta = meta.copy()
            meta["task"] = "sync_dir"
            chunk = {}
# ----------------------
            #task_queue(file_q_name).add(sync_dir, meta)
            meta['queue_name'] = file_q_name
            enqueue_task(sync_dir, meta)
            itr = scandir(path)
# ----------------------

            if meta["profile"]:
                config = meta["config"]
                profile_log = config.get("profile")
                profile_logger = sync_logging.get_sync_logger(profile_log)
                task_id = task_cls.request.id

                profile_logger.info("list_dir_prerun", event_id=task_id + ":list_dir", event_name="list_dir", hostname=task_cls.request.hostname, index=current_process().index)
                itr = list(itr)
                if meta["profile"]:
                    profile_logger.info("list_dir_postrun", event_id=task_id + ":list_dir", event_name="list_dir", hostname=task_cls.request.hostname, index=current_process().index)

            for obj in itr:
                obj_stats = {}
# ----------------------
                full_path = os.path.abspath(obj.path)
                mode = obj.stat(follow_symlinks=False).st_mode

                if self.exclude_file_type(dir_regex, file_regex, full_path, logger, mode):
                    continue

                if not obj.is_symlink() and not bool(mode & stat.S_IRGRP):
                    logger.error('physical path is not readable [{0}]'.format(full_path))
                    continue

                if obj.is_dir() and not obj.is_symlink() and not obj.is_file():
                    sync_dir_meta = meta.copy()
                    sync_dir_meta['path'] = full_path
                    sync_dir_meta['mtime'] = obj.stat(follow_symlinks=False).st_mtime
                    sync_dir_meta['ctime'] = obj.stat(follow_symlinks=False).st_ctime
                    #task_queue(path_q_name).add(sync_path, sync_dir_meta)
                    sync_dir_meta['queue_name'] = path_q_name
                    enqueue_task(sync_path, sync_dir_meta)
                    continue

                obj_stats['is_link'] = obj.is_symlink()
                obj_stats['is_socket'] = stat.S_ISSOCK(mode)
                obj_stats['mtime'] = obj.stat(follow_symlinks=False).st_mtime
                obj_stats['ctime'] = obj.stat(follow_symlinks=False).st_ctime
                obj_stats['size'] = obj.stat(follow_symlinks=False).st_size
# ----------------------

                # add object stat dict to the chunk dict
                chunk[full_path] = obj_stats

                # Launch async job when enough objects are ready to be sync'd
                files_per_task = meta.get('files_per_task')
                if len(chunk) >= files_per_task:
                    sync_files_meta = meta.copy()
                    sync_files_meta['chunk'] = chunk
                    #task_queue(file_q_name).add(sync_files, sync_files_meta)
                    sync_files_meta['queue_name'] = file_q_name
                    enqueue_task(sync_files, sync_files_meta)
                    chunk.clear()

            if len(chunk) > 0:
                sync_files_meta = meta.copy()
                sync_files_meta['chunk'] = chunk
                #task_queue(file_q_name).add(sync_files, sync_files_meta)
                sync_files_meta['queue_name'] = file_q_name
                enqueue_task(sync_files, sync_files_meta)
                chunk.clear()

        except Exception as err:
            event_handler = custom_event_handler(meta)
            retry_countdown = event_handler.delay(task_cls.request.retries + 1)
            max_retries = event_handler.max_retries()
            raise task_cls.retry(max_retries=max_retries, exc=err, countdown=retry_countdown)

    def sync_entry(self, task_cls, meta, cls, datafunc, metafunc):
        task = meta["task"]
        path = meta["path"]
        root = meta["root"]
        target = meta["target"]
        config = meta["config"]
        logging_config = config["log"]
        ignore_cache = meta["ignore_cache"]
        logger = sync_logging.get_sync_logger(logging_config)

        event_handler = custom_event_handler(meta)
        max_retries = event_handler.max_retries()

        lock = None
        logger.info("synchronizing " + cls + ". path = " + path)

        def is_unicode_encode_error_path(path):
            # Attempt to encode full physical path on local filesystem
            # Special handling required for non-encodable strings which raise UnicodeEncodeError
            try:
                _ = path.encode('utf8')
            except UnicodeEncodeError:
                return True
            return False

        if is_unicode_encode_error_path(path):
            abspath = os.path.abspath(path)
            path = os.path.dirname(abspath)
            utf8_escaped_abspath = abspath.encode('utf8', 'surrogateescape')
            b64_path_str = base64.b64encode(utf8_escaped_abspath)

            unicode_error_filename = 'irods_UnicodeEncodeError_' + str(b64_path_str.decode('utf8'))

            logger.warning('sync_entry raised UnicodeEncodeError while syncing path:' + str(utf8_escaped_abspath))

            meta['path'] = path
            meta['b64_path_str'] = b64_path_str
            meta['unicode_error_filename'] = unicode_error_filename

            sync_key = str(b64_path_str.decode('utf8')) + ":" + target
        else:
            sync_key = path + ":" + target

        try:
            r = get_redis(config)
            lock = redis_lock.Lock(r, "sync_" + cls + ":"+sync_key)
            lock.acquire()

            sync_time_handle = sync_time_key_handle(r, sync_key)
            if not ignore_cache:
                sync_time = sync_time_handle.get_value()
            else:
                sync_time = None

            mtime = meta.get("mtime")
            if mtime is None:
                mtime = getmtime(path)

            ctime = meta.get("ctime")
            if ctime is None:
                ctime = getctime(path)

            if sync_time is not None and mtime < sync_time and ctime < sync_time:
                logger.info("succeeded_" + cls + "_has_not_changed", task=task, path=path)
            else:
                t = datetime.now().timestamp()
                logger.info("synchronizing " + cls, path=path, t0=sync_time, t=t, ctime=ctime)
                meta2 = meta.copy()
                if path == root:
                    if 'unicode_error_filename' in meta:
                        target2 = join(target, meta['unicode_error_filename'])
                    else:
                        target2 = target
                else:
# ----------------------
                    target2 = join(target, relpath(path, start=root))
# ----------------------
                meta2["target"] = target2
                if sync_time is None or mtime >= sync_time:
                    datafunc(meta2, logger, True)
                    logger.info("succeeded", task=task, path=path)
                else:
                    metafunc(meta2, logger)
                    logger.info("succeeded_metadata_only", task=task, path=path)
                sync_time_handle.set_value(str(t))
        except Exception as err:
            event_handler = custom_event_handler(meta)
            retry_countdown = event_handler.delay(task_cls.request.retries + 1)
            raise task_cls.retry(max_retries=max_retries, exc=err, countdown=retry_countdown)
        finally:
            if lock is not None:
                lock.release()

class s3_scanner(scanner):
    def __init__(self, meta):
        super(s3_scanner, self).__init__(meta)
        #self.s3_keypair = meta.get('s3_keypair')
        self.proxy_url = meta.get('s3_proxy_url')
        self.endpoint_domain = meta.get('s3_endpoint_domain')
        self.s3_access_key = meta.get('s3_access_key')
        self.s3_secret_key = meta.get('s3_secret_key')

    def sync_path(self, task_cls, meta):
        task = meta["task"]
        path = meta["path"]
        path_q_name = meta["path_queue"]
        file_q_name = meta["file_queue"]
        config = meta["config"]
        logging_config = config["log"]
        exclude_file_name = meta['exclude_file_name']
        exclude_directory_name = meta['exclude_directory_name']

        logger = sync_logging.get_sync_logger(logging_config)

        file_regex = [ re.compile(r) for r in exclude_file_name ]
        dir_regex  = [ re.compile(r) for r in exclude_directory_name ]

        try:
            logger.info("walk dir", path=path)
            meta = meta.copy()
            meta["task"] = "sync_dir"
            chunk = {}

# ----------------------
            # TODO: #64 - Need to somehow trigger sync_dir for folders without stat'ing for PEPs
            # instantiate s3 client
            proxy_url = meta.get('s3_proxy_url')
            if proxy_url is None:
                httpClient = None
            else:
                import urllib3
                httpClient = urllib3.ProxyManager(
                                        proxy_url,
                                        timeout=urllib3.Timeout.DEFAULT_TIMEOUT,
                                        cert_reqs='CERT_REQUIRED',
                                        retries=urllib3.Retry(
                                            total=5,
                                            backoff_factor=0.2,
                                            status_forcelist=[500, 502, 503, 504]
                                        )
                             )
            endpoint_domain = meta.get('s3_endpoint_domain')
            s3_region_name = meta.get('s3_region_name')
            s3_access_key = meta.get('s3_access_key')
            s3_secret_key = meta.get('s3_secret_key')
            s3_secure_connection = meta.get('s3_secure_connection')
            if s3_secure_connection is None:
                s3_secure_connection = True
            client = Minio(
                         endpoint_domain,
                         region=s3_region_name,
                         access_key=s3_access_key,
                         secret_key=s3_secret_key,
                         secure=s3_secure_connection,
                         http_client=httpClient)

            # Split provided path into bucket and source folder "prefix"
            path_list = path.lstrip('/').split('/', 1)
            bucket_name = path_list[0]
            if len(path_list) == 1:
                prefix = ''
            else:
                prefix = path_list[1]
            meta['root'] = bucket_name
            meta['s3_prefix'] = prefix
            itr = client.list_objects(bucket_name, prefix=prefix, recursive=True)
# ----------------------

            if meta["profile"]:
                config = meta["config"]
                profile_log = config.get("profile")
                profile_logger = sync_logging.get_sync_logger(profile_log)
                task_id = task_cls.request.id

                profile_logger.info("list_dir_prerun", event_id=task_id + ":list_dir", event_name="list_dir", hostname=task_cls.request.hostname, index=current_process().index)
                itr = list(itr)
                if meta["profile"]:
                    profile_logger.info("list_dir_postrun", event_id=task_id + ":list_dir", event_name="list_dir", hostname=task_cls.request.hostname, index=current_process().index)

            for obj in itr:
                obj_stats = {}
# ----------------------
                if obj.object_name.endswith('/'):
                    continue
                full_path = obj.object_name
                obj_stats['is_link'] = False
                obj_stats['is_socket'] = False
                obj_stats['mtime'] = obj.last_modified.timestamp()
                obj_stats['ctime'] = obj.last_modified.timestamp()
                obj_stats['size'] = obj.size
# ----------------------

                # add object stat dict to the chunk dict
                chunk[full_path] = obj_stats

                # Launch async job when enough objects are ready to be sync'd
                files_per_task = meta.get('files_per_task')
                if len(chunk) >= files_per_task:
                    sync_files_meta = meta.copy()
                    sync_files_meta['chunk'] = chunk
                    #task_queue(file_q_name).add(sync_files, sync_files_meta)
                    sync_files_meta['queue_name'] = file_q_name
                    enqueue_task(sync_files, sync_files_meta)
                    chunk.clear()

            if len(chunk) > 0:
                sync_files_meta = meta.copy()
                sync_files_meta['chunk'] = chunk
                #task_queue(file_q_name).add(sync_files, sync_files_meta)
                sync_files_meta['queue_name'] = file_q_name
                enqueue_task(sync_files, sync_files_meta)
                chunk.clear()

        except Exception as err:
            event_handler = custom_event_handler(meta)
            retry_countdown = event_handler.delay(task_cls.request.retries + 1)
            max_retries = event_handler.max_retries()
            raise task_cls.retry(max_retries=max_retries, exc=err, countdown=retry_countdown)

    def sync_entry(self, task_cls, meta, cls, datafunc, metafunc):
        task = meta["task"]
        path = meta["path"]
        root = meta["root"]
        target = meta["target"]
        config = meta["config"]
        logging_config = config["log"]
        ignore_cache = meta["ignore_cache"]
        logger = sync_logging.get_sync_logger(logging_config)

        event_handler = custom_event_handler(meta)
        max_retries = event_handler.max_retries()

        lock = None
        logger.info("synchronizing " + cls + ". path = " + path)

        def is_unicode_encode_error_path(path):
            # Attempt to encode full physical path on local filesystem
            # Special handling required for non-encodable strings which raise UnicodeEncodeError
            try:
                _ = path.encode('utf8')
            except UnicodeEncodeError:
                return True
            return False

        if is_unicode_encode_error_path(path):
            abspath = os.path.abspath(path)
            path = os.path.dirname(abspath)
            utf8_escaped_abspath = abspath.encode('utf8', 'surrogateescape')
            b64_path_str = base64.b64encode(utf8_escaped_abspath)

            unicode_error_filename = 'irods_UnicodeEncodeError_' + str(b64_path_str.decode('utf8'))

            logger.warning('sync_entry raised UnicodeEncodeError while syncing path:' + str(utf8_escaped_abspath))

            meta['path'] = path
            meta['b64_path_str'] = b64_path_str
            meta['unicode_error_filename'] = unicode_error_filename

            sync_key = str(b64_path_str.decode('utf8')) + ":" + target
        else:
            sync_key = path + ":" + target

        try:
            r = get_redis(config)
            lock = redis_lock.Lock(r, "sync_" + cls + ":"+sync_key)
            lock.acquire()

            sync_time_handle = sync_time_key_handle(r, sync_key)
            if not ignore_cache:
                sync_time = sync_time_handle.get_value()
            else:
                sync_time = None

            mtime = meta.get("mtime")
            if mtime is None:
                mtime = getmtime(path)

            ctime = meta.get("ctime")
            if ctime is None:
                ctime = getctime(path)

            if sync_time is not None and mtime < sync_time and ctime < sync_time:
                logger.info("succeeded_" + cls + "_has_not_changed", task=task, path=path)
            else:
                t = datetime.now().timestamp()
                logger.info("synchronizing " + cls, path=path, t0=sync_time, t=t, ctime=ctime)
                meta2 = meta.copy()
                if path == root:
                    if 'unicode_error_filename' in meta:
                        target2 = join(target, meta['unicode_error_filename'])
                    else:
                        target2 = target
                else:
# ----------------------
                    # Strip prefix from S3 path
                    prefix = meta['s3_prefix']
                    reg_path = path[path.index(prefix) + len(prefix):].strip('/')
                    # Construct S3 "logical path"
                    target2 = join(target, reg_path)
                    # Construct S3 "physical path" as: /bucket/objectname
                    meta2['path'] = '/' + join(root, path)
# ----------------------
                meta2["target"] = target2
                if sync_time is None or mtime >= sync_time:
                    datafunc(meta2, logger, True)
                    logger.info("succeeded", task=task, path=path)
                else:
                    metafunc(meta2, logger)
                    logger.info("succeeded_metadata_only", task=task, path=path)
                sync_time_handle.set_value(str(t))
        except Exception as err:
            event_handler = custom_event_handler(meta)
            retry_countdown = event_handler.delay(task_cls.request.retries + 1)
            raise task_cls.retry(max_retries=max_retries, exc=err, countdown=retry_countdown)
        finally:
            if lock is not None:
                lock.release()

def scanner_factory(meta):
    if meta.get('s3_keypair'):
        return s3_scanner(meta)
    return filesystem_scanner(meta)
