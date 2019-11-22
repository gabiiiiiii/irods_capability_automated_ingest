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
from . import sync_logging
from .sync_utils import app, get_redis
from .custom_event_handler import custom_event_handler
from .redis_key import sync_time_key_handle
from .utils import enqueue_task
from . import sync_logging
import irods_capability_automated_ingest.irods_task
#from .task_queue import task_queue
from minio import Minio
from billiard import current_process
import base64
import re

# Base class for scanning some kind of data storage -- this should not be instantiated
class scanner_base(object):
    def __init__(self, meta):
        self.meta = meta.copy()

    # Get iterable for walking the filesystem or object store
    def get_iter(self, meta):
        pass

    # Enqueues subdirectories and files for synchronization and returns object info according to the underlying data store
    # Object info should be a dictionary with the following keys:
    #
    # is_link - Is the file a symlink?
    # is_socket - Is the file a socket?
    # mtime - mtime of the file
    # ctime - ctime of the file
    # size - size of the file
    def recursively_sync_obj(self, meta, obj):
        pass

    # Get the absolute path of the object according to the underlying filesystem structure
    def get_full_path_for_obj(self, obj):
        pass

    # Constructs a full source path for the entry
    def get_physical_path_for_entry(self, meta, source_physical_path, source_physical_path_top_level):
        return source_physical_path

    # Constructs a full destination path for the entry
    def get_logical_path_for_entry(self, meta, destination_logical_path, source_physical_path, source_physical_path_top_level):
        return destination_logical_path

    # Enqueues task which will synchronize a set of 1 or more files
    def syncronize_chunk(self, meta, chunk):
        file_q_name = meta["file_queue"]
        sync_files_meta = meta.copy()
        sync_files_meta['chunk'] = chunk
        #task_queue(file_q_name).add(sync_files, sync_files_meta)
        sync_files_meta['queue_name'] = file_q_name
        enqueue_task(irods_capability_automated_ingest.irods_task.sync_files, sync_files_meta)

    def profile_list_dir(self, meta, itr):
        config = meta["config"]
        profile_log = config.get("profile")
        profile_logger = sync_logging.get_sync_logger(profile_log)
        task_id = task_cls.request.id

        profile_logger.info("list_dir_prerun", event_id=task_id + ":list_dir", event_name="list_dir", hostname=task_cls.request.hostname, index=current_process().index)
        itr = list(itr)
        profile_logger.info("list_dir_postrun", event_id=task_id + ":list_dir", event_name="list_dir", hostname=task_cls.request.hostname, index=current_process().index)

    # Walks the given source path and synchronizes all contained files and directories
    # Subdirectories are recursively enqueued for asynchronous synchronization
    def sync_path(self, task_cls, meta):
        path = meta["path"]
        config = meta["config"]
        logging_config = config["log"]

        logger = sync_logging.get_sync_logger(logging_config)

        try:
            logger.info("walk dir", path=path)
            meta = meta.copy()
            meta["task"] = "sync_dir"
            chunk = {}

            itr = self.get_iter(meta)

            # TODO: What is this
            #if meta["profile"]:
                #self.profile_list_dir(meta, itr)

            for obj in itr:
                obj_stats = self.recursively_sync_obj(meta, obj)
                if obj_stats is None:
                    continue

                # Add object stat dict to the chunk dict
                full_path = self.get_full_path_for_obj(obj)
                print('full_path:['+full_path+']')
                chunk[full_path] = obj_stats

                # Launch async job when enough objects are ready to be sync'd
                files_per_task = meta.get('files_per_task')
                if len(chunk) >= files_per_task:
                    self.syncronize_chunk(meta, chunk)
                    chunk.clear()

            # Leftovers
            if len(chunk) > 0:
                self.syncronize_chunk(meta, chunk)
                chunk.clear()

        except Exception as err:
            event_handler = custom_event_handler(meta)
            retry_countdown = event_handler.delay(task_cls.request.retries + 1)
            max_retries = event_handler.max_retries()
            raise task_cls.retry(max_retries=max_retries, exc=err, countdown=retry_countdown)

    def get_key_for_sync_time(self, meta, source_physical_path, destination_logical_path):
        def is_unicode_encode_error_path(source_physical_path):
            # Attempt to encode full physical path on local filesystem
            # Special handling required for non-encodable strings which raise UnicodeEncodeError
            try:
                _ = source_physical_path.encode('utf8')
            except UnicodeEncodeError:
                return True
            return False

        if is_unicode_encode_error_path(source_physical_path) is False:
            return source_physical_path + ":" + destination_logical_path

        abspath = os.path.abspath(source_physical_path)
        source_physical_path = os.path.dirname(abspath)
        utf8_escaped_abspath = abspath.encode('utf8', 'surrogateescape')
        b64_path_str = base64.b64encode(utf8_escaped_abspath)

        unicode_error_filename = 'irods_UnicodeEncodeError_' + str(b64_path_str.decode('utf8'))

        logger.warning('sync_entry raised UnicodeEncodeError while syncing path:' + str(utf8_escaped_abspath))

        meta['path'] = source_physical_path
        meta['b64_path_str'] = b64_path_str
        meta['unicode_error_filename'] = unicode_error_filename

        return str(b64_path_str.decode('utf8')) + ":" + destination_logical_path

    def sync_entry(self, task_cls, meta, entry_type, irods_data_sync_function, irods_metadata_sync_function):
        task = meta["task"]
        config = meta["config"]
        logger = sync_logging.get_sync_logger(config['log'])

        source_physical_path = meta["path"]
        source_physical_path_top_level = meta["root"]
        destination_logical_path = meta["target"]

        sync_key = self.get_key_for_sync_time(meta, source_physical_path, destination_logical_path)

        lock = None
        logger.info("synchronizing " + entry_type + ". path = " + source_physical_path)

        try:
            r = get_redis(config)
            lock = redis_lock.Lock(r, "sync_" + entry_type + ":"+sync_key)
            lock.acquire()

            sync_time_handle = sync_time_key_handle(r, sync_key)
            if not meta["ignore_cache"]:
                sync_time = sync_time_handle.get_value()
            else:
                sync_time = None

            mtime = meta.get("mtime")
            if mtime is None:
                mtime = getmtime(source_physical_path)

            ctime = meta.get("ctime")
            if ctime is None:
                ctime = getctime(source_physical_path)

            if sync_time is not None and mtime < sync_time and ctime < sync_time:
                logger.info("succeeded_" + entry_type + "_has_not_changed", task=task, path=source_physical_path)
            else:
                t = datetime.now().timestamp()
                logger.info("synchronizing " + entry_type, path=source_physical_path, t0=sync_time, t=t, ctime=ctime)
                meta2 = meta.copy()
                meta2['path'] = self.get_physical_path_for_entry(meta, source_physical_path, source_physical_path_top_level)
                meta2["target"] = self.get_logical_path_for_entry(meta, destination_logical_path, source_physical_path, source_physical_path_top_level)
                if sync_time is None or mtime >= sync_time:
                    irods_data_sync_function(meta2)
                    logger.info("succeeded", task=task, path=source_physical_path)
                else:
                    irods_metadata_sync_function(meta2)
                    logger.info("succeeded_metadata_only", task=task, path=source_physical_path)
                sync_time_handle.set_value(str(t))
        except Exception as err:
            event_handler = custom_event_handler(meta)
            max_retries = event_handler.max_retries()
            retry_countdown = event_handler.delay(task_cls.request.retries + 1)
            raise task_cls.retry(max_retries=max_retries, exc=err, countdown=retry_countdown)
        finally:
            if lock is not None:
                lock.release()

class filesystem_scanner(scanner_base):
    def __init__(self, meta):
        super().__init__(meta)

    def exclude_file_type(self, meta, dir_regex, file_regex, full_path, mode=None):
        exclude_type_list = meta['exclude_file_type']
        logger = sync_logging.get_sync_logger(meta['config']['log'])
        if len(exclude_type_list) <= 0 and None == dir_regex and None == file_regex:
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
            if 'regular' in exclude_type_list or file_match:
                ret_val = True
        elif stat.S_ISDIR(mode):
            if 'directory' in exclude_type_list or dir_match:
                ret_val = True
        elif stat.S_ISCHR(mode):
            if 'character' in exclude_type_list or file_match:
                ret_val = True
        elif stat.S_ISBLK(mode):
            if 'block' in exclude_type_list or file_match:
                ret_val = True
        elif stat.S_ISSOCK(mode):
            if 'socket' in exclude_type_list or file_match:
                ret_val = True
        elif stat.S_ISFIFO(mode):
            if 'pipe' in exclude_type_list or file_match:
                ret_val = True
        elif stat.S_ISLNK(mode):
            if 'link' in exclude_type_list or file_match:
                ret_val = True

        return ret_val

    # Override
    def get_iter(self, meta):
        #task_queue(file_q_name).add(irods_task.sync_dir, meta)
        meta['queue_name'] = meta['file_queue']
        enqueue_task(irods_capability_automated_ingest.irods_task.sync_dir, meta)
        return scandir(meta['path'])

    # Override
    def get_full_path_for_obj(self, obj):
        return os.path.abspath(obj.path)

    # Override
    def recursively_sync_obj(self, meta, obj):
        exclude_file_name = meta['exclude_file_name']
        exclude_directory_name = meta['exclude_directory_name']
        file_regex = [ re.compile(r) for r in exclude_file_name ]
        dir_regex  = [ re.compile(r) for r in exclude_directory_name ]

        full_path = self.get_full_path_for_obj(obj)
        mode = obj.stat(follow_symlinks=False).st_mode
        if self.exclude_file_type(meta, dir_regex, file_regex, full_path, mode):
            return None

        if not obj.is_symlink() and not bool(mode & stat.S_IRGRP):
            logger = sync_logging.get_sync_logger(meta['config']['log'])
            logger.error('physical path is not readable [{0}]'.format(full_path))
            return None

        if obj.is_dir() and not obj.is_symlink() and not obj.is_file():
            print('obj ['+full_path+'] is dir')
            sync_dir_meta = meta.copy()
            sync_dir_meta['path'] = full_path
            sync_dir_meta['mtime'] = obj.stat(follow_symlinks=False).st_mtime
            sync_dir_meta['ctime'] = obj.stat(follow_symlinks=False).st_ctime
            #path_q_name = meta["path_queue"]
            #task_queue(path_q_name).add(irods_task.sync_path, sync_dir_meta)
            sync_dir_meta['queue_name'] = meta['path_queue']
            enqueue_task(irods_capability_automated_ingest.irods_task.sync_path, sync_dir_meta)
            return None

        obj_stats = {}
        obj_stats['is_link'] = obj.is_symlink()
        obj_stats['is_socket'] = stat.S_ISSOCK(mode)
        obj_stats['mtime'] = obj.stat(follow_symlinks=False).st_mtime
        obj_stats['ctime'] = obj.stat(follow_symlinks=False).st_ctime
        obj_stats['size'] = obj.stat(follow_symlinks=False).st_size
        return obj_stats

    # Override
    def get_logical_path_for_entry(self, meta, destination_logical_path, source_physical_path, source_physical_path_top_level):
        if source_physical_path == source_physical_path_top_level:
            if 'unicode_error_filename' in meta:
                return join(destination_logical_path, meta['unicode_error_filename'])
            else:
                return destination_logical_path
        return join(destination_logical_path, relpath(source_physical_path, start=source_physical_path_top_level))

class s3_scanner(scanner_base):
    def __init__(self, meta):
        super().__init__(meta)
        #self.s3_keypair = meta.get('s3_keypair')
        self.proxy_url = meta.get('s3_proxy_url')
        self.endpoint_domain = meta.get('s3_endpoint_domain')
        self.s3_access_key = meta.get('s3_access_key')
        self.s3_secret_key = meta.get('s3_secret_key')

    # Create S3 client - Minio-specific for now
    def s3_client_factory(self):
        if self.proxy_url is None:
            httpClient = None
        else:
            import urllib3
            httpClient = urllib3.ProxyManager(
                                    self.proxy_url,
                                    timeout=urllib3.Timeout.DEFAULT_TIMEOUT,
                                    cert_reqs='CERT_REQUIRED',
                                    retries=urllib3.Retry(
                                        total=5,
                                        backoff_factor=0.2,
                                        status_forcelist=[500, 502, 503, 504]
                                    )
                         )
        return Minio(self.endpoint_domain,
                     access_key=self.s3_access_key,
                     secret_key=self.s3_secret_key,
                     http_client=httpClient)

    # Splits source path for S3 into bucket and prefix; returns pair
    def split_s3_source_path(self, source_path):
        path_list = path.lstrip('/').split('/', 1)
        bucket_name = path_list[0]
        if len(path_list) == 1:
            object_prefix = ''
        else:
            object_prefix = path_list[1]
        return bucket_name, object_prefix

    # Override
    def get_iter(self, meta):
        client = s3_client_factory()
        bucket_name, object_prefix = self.split_s3_source_path(meta['path'])
        meta['root'] = bucket_name
        meta['s3_prefix'] = object_prefix
        itr = self.get_object_list_for_bucket(client, bucket, object_prefix)
        itr = client.list_objects_v2(bucket_name, prefix=object_prefix, recursive=True)

    # Override
    def get_full_path_for_obj(self, obj):
        return obj.object_name

    # Override
    def recursively_sync_obj(self, meta, obj):
        obj_stats = {}
        if obj.object_name.endswith('/'):
            return None
        obj_stats['is_link'] = False
        obj_stats['is_socket'] = False
        obj_stats['mtime'] = obj.last_modified.timestamp()
        obj_stats['ctime'] = obj.last_modified.timestamp()
        obj_stats['size'] = obj.size
        return obj_stats

    # Override
    def get_physical_path_for_entry(self, meta, source_physical_path, source_physical_path_top_level):
        # Construct S3 "physical path" as: /bucket/objectname
        return '/' + join(source_physical_path_top_level, source_physical_path)

    # Override
    def get_logical_path_for_entry(self, meta, destination_logical_path, source_physical_path, source_physical_path_top_level):
        if source_physical_path == source_physical_path_top_level and 'unicode_error_filename' in meta:
            return join(destination_logical_path, meta['unicode_error_filename'])

        # Strip prefix from S3 path
        prefix = meta['s3_prefix']
        reg_path = source_physical_path[source_physical_path.index(prefix) + len(prefix):].strip('/')
        return join(destination_logical_path, reg_path)

# TODO: Look into passing a string as a type... modularize scanners and import as needed
# TODO: Talk to Dan about eval()
def scanner_factory(meta):
    if meta.get('s3_keypair'):
        return s3_scanner(meta)
    return filesystem_scanner(meta)

