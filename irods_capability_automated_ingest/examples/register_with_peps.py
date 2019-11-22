from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation
from .. import sync_logging

class event_handler(Core):
    @staticmethod
    def operation(session, meta, **options):
        return Operation.REGISTER_SYNC

    @staticmethod
    def pre_data_obj_create(meta, session, **options):
        logical_path = meta['target']
        logger = sync_logging.get_sync_logger(meta['config']['log'])
        logger.info('pre_data_obj_create:['+logical_path+']')

    @staticmethod
    def post_data_obj_create(meta, session, **options):
        logical_path = meta['target']
        logger = sync_logging.get_sync_logger(meta['config']['log'])
        logger.info('post_data_obj_create:['+logical_path+']')

    @staticmethod
    def pre_coll_create(meta, session, **options):
        logical_path = meta['target']
        logger = sync_logging.get_sync_logger(meta['config']['log'])
        logger.info('pre_coll_create:['+logical_path+']')

    @staticmethod
    def post_coll_create(meta, session, **options):
        logical_path = meta['target']
        logger = sync_logging.get_sync_logger(meta['config']['log'])
        logger.info('post_coll_create:['+logical_path+']')

    @staticmethod
    def pre_data_obj_modify(meta, session, **options):
        logical_path = meta['target']
        logger = sync_logging.get_sync_logger(meta['config']['log'])
        logger.info('pre_data_obj_modify:['+logical_path+']')

    @staticmethod
    def post_data_obj_modify(meta, session, **options):
        logical_path = meta['target']
        logger = sync_logging.get_sync_logger(meta['config']['log'])
        logger.info('post_data_obj_modify:['+logical_path+']')

    @staticmethod
    def pre_coll_modify(meta, session, **options):
        logical_path = meta['target']
        logger = sync_logging.get_sync_logger(meta['config']['log'])
        logger.info('pre_coll_modify:['+logical_path+']')

    @staticmethod
    def post_coll_modify(meta, session, **options):
        logical_path = meta['target']
        logger = sync_logging.get_sync_logger(meta['config']['log'])
        logger.info('post_coll_modify:['+logical_path+']')
