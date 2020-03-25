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
        args_list = list(args[1:])
        if len(args_list) > 0:
            for index, arg in enumerate(args_list):
                if getattr(arg, 'to_dict', None):
                    args_list[index] = arg.to_dict()
            args_map_list = list((map(lambda x: json.loads(x), list(args_list))))
            args_map_list.insert(0, args[0])
            args = tuple(args_map_list)
        ser = dict(data=func(*args, **kwargs))
        return json.dumps(ser)
    return ser_wrapper
