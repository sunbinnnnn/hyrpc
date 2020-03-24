import json

from thrift_connector import connection_pool

from hyrpc.exception import NotFound
from hyrpc.exception import InvalidParameterValue


def get_rcp_api(svc_endpoint, service_ins):
    return connection_pool.ClientPool(
        service_ins,
        svc_endpoint.split(':')[0],
        int(svc_endpoint.split(':')[1]),
        connection_class=connection_pool.ThriftClient,
        timeout=300
        )


class ConsumerManagerBase(object):

    service = None

    def __init__(self, svc_endpoint):
        if not self.service:
            raise NotFound("Rpc service not found ")
        self.rpc_api = get_rcp_api(svc_endpoint, self.service)

    def bye(self, func, *args, **kwargs):
        rpc_func = getattr(self.rpc_api, func)
        if args:
            raise InvalidParameterValue(
                "Unknown args %s, use kwargs instead!" % str(args))
        for k, v in kwargs.items():
            if getattr(v, 'to_dict', None):
                kwargs[k] = json.dumps(v.to_dict())
                continue
            kwargs[k] = json.dumps(v)
        call_res = rpc_func(**kwargs)
        if call_res:
            return json.loads(rpc_func(**kwargs)).get('data')
