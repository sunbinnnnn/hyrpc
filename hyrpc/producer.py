import json
import functools

from oslo_log import log as logging
from oslo_utils import importutils


LOG = logging.getLogger(__name__)


class BaseProducer(object):
    service = None

    @classmethod
    def processor(cls):
        conductor_class = importutils.try_import(cls.service)
        return conductor_class.Processor(cls())


def recall(func):
    """Set recall serialization for RPC"""
    @functools.wraps(func)
    def ser_wrapper(*args, **kwargs):
        ser = dict(data=func(*args, **kwargs))
        return json.dumps(ser)
    return ser_wrapper
