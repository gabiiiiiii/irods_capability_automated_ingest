from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation
from irods.meta import iRODSMeta
import subprocess

class event_handler(Core):

    @staticmethod
    def operation(session, meta, **options):
        return Operation.PUT


    @staticmethod
    def post_data_obj_create(hdlr_mod, logger, session, meta, **options):
        args = ['stat', '--printf', '%04a,%U,%G,%x,%y', meta['path']]
        out, err = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        s = str(out.decode('UTF-8')).split(',')
        print(s)
        obj = session.data_objects.get(meta['target'])
        obj.metadata.add("filesystem::perms", s[0], '')
        obj.metadata.add("filesystem::owner", s[1], '')
        obj.metadata.add("filesystem::group", s[2], '')
        obj.metadata.add("filesystem::atime", s[3], '')
        obj.metadata.add("filesystem::mtime", s[4], '')
        session.cleanup()
