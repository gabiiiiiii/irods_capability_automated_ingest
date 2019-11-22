import importlib
from . import sync_logging

class custom_event_handler(object):
    def __init__(self, meta):
        self.meta = meta.copy()
        self.logger = sync_logging.get_sync_logger(self.meta['config']['log'])

    def get_module(self):
        key = 'event_handler'
        h = self.meta.get(key)
        if h is None:
            return None
        return getattr(importlib.import_module(h), key, None)

    def hasattr(self, attr):
        module = self.get_module()
        return module is not None and hasattr(module, attr)

    def call(self, event_function_name, irods_sync_function, *args, **options):
        module = self.get_module()
        if self.hasattr(event_function_name):
            self.logger.debug("calling [" + event_function_name + "] in event handler: args = " + str(args) + ", options = " + str(options))
            getattr(module, event_function_name)(irods_sync_function, *args, **options)
        else:
            irods_sync_function(*args, **options)

    # attribute getters
    def max_retries(self):
        if self.hasattr('max_retries'):
            module = self.get_module()
            return module.max_retries(module, self.logger, self.meta)
        return 0

    def timeout(self):
        if self.hasattr('timeout'):
            module = self.get_module()
            return module.timeout(module, self.logger, self.meta)
        return 3600

    def delay(self, retries):
        if self.hasattr('delay'):
            module = self.get_module()
            return module.delay(module, self.logger, self.meta, retries)
        return 0

    def operation(self, session, **options):
        if self.hasattr("operation"):
            return self.get_module().operation(session, self.meta, **options)
        #return Operation.REGISTER_SYNC
        return None

    def to_resource(self, session, **options):
        if self.hasattr("to_resource"):
            return self.get_module().to_resource(session, self.meta, **options)
        return None

    def target_path(self, session, **options):
        if self.hasattr("target_path"):
            return self.get_module().target_path(session, self.meta, **options)
        return None

