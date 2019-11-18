from . import sync_logging
from .sync_job import sync_job
from .custom_event_handler import custom_event_handler
from uuid import uuid1

from enum import Enum

class Operation(Enum):
    REGISTER_SYNC = 0
    REGISTER_AS_REPLICA_SYNC = 1
    PUT = 2
    PUT_SYNC = 3
    PUT_APPEND = 4
    NO_OP = 5
