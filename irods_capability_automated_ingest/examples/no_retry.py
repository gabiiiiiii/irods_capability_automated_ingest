from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation
from irods_capability_automated_ingest.sync_utils import get_redis

class event_handler(Core):

    @staticmethod
    def operation(session, meta, **options):
        return Operation.NO_OP

    @staticmethod
    def pre_data_obj_create(meta, session, **options):
        target = meta["target"]
        path = meta["path"]

        r = get_redis(meta['config'])
        failures = r.get("failures:"+path)
        if failures is None:
            failures = 0

        r.incr("failures:"+path)

        if failures == 0:
            raise RuntimeError("no failures")





