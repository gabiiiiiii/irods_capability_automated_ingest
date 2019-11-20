import importlib

class custom_event_handler(object):
    def __init__(self, meta):
        self.meta = meta.copy()
        self.logger = self.meta['config']['log']

    def get_module(self):
        key = 'event_handler'
        h = self.meta.get(key)
        if h is None:
            return None
        return getattr(importlib.import_module(h), key, None)

    def hasattr(self, attr):
        module = self.get_module()
        return module is not None and hasattr(module, attr)

    def call(self, hdlr, logger, func, *args, **options):
        module = self.get_module()
        if self.hasattr(hdlr):
            logger.debug("calling " + hdlr + " event handler: args = " + str(args) + ", options = " + str(options))
            getattr(module, hdlr)(func, *args, **options)
        else:
            func(*args, **options)

    # attribute getters
    def max_retries(self):
        module = self.get_module()
        if self.hasattr('max_retries'):
            return module.max_retries(module, self.logger, self.meta)
        return 0

    def timeout(self):
        module = self.get_module()
        if self.hasattr('timeout'):
            return module.timeout(module, self.logger, self.meta)
        return 3600

    def delay(self, retries):
        module = self.get_module()
        if self.hasattr('delay'):
            return module.delay(module, self.logger, self.meta, retries)
        return 0

    def operation(self, session, **options):
        module = self.get_module()
        if self.hasattr("operation"):
            return module.operation(session, self.meta, **options)
        #return Operation.REGISTER_SYNC
        return None

    def to_resource(self, session, **options):
        module = self.get_module()
        if self.hasattr("to_resource"):
            return module.to_resource(session, self.meta, **options)
        return None

    def target_path(self, session, **options):
        module = self.get_module()
        if self.hasattr("target_path"):
            return module.target_path(session, self.meta, **options)
        return None

