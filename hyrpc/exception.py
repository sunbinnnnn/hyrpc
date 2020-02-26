import sys
import six

from oslo_config import cfg
from oslo_log import log as logging
from hyrpc.i18n import _, _LE


LOG = logging.getLogger(__name__)

exc_log_opts = [
    cfg.BoolOpt('fatal_exception_format_errors',
                default=False,
                help='Make exception message format errors fatal'),
]


CONF = cfg.CONF
CONF.register_opts(exc_log_opts)


class HyRpcException(Exception):
    """Base Franxx Exception

    To correctly use this class, inherit from it and define
    a 'msg_fmt' property. That msg_fmt will get printf'd
    with the keyword arguments provided to the constructor.

    """
    msg_fmt = _("An unknown exception occurred.")
    code = 500
    headers = {}
    safe = False

    def __init__(self, message=None, **kwargs):
        self.kwargs = kwargs

        if 'code' not in self.kwargs:
            try:
                self.kwargs['code'] = self.code
            except AttributeError:
                pass

        if not message:
            try:
                message = self.msg_fmt % kwargs

            except Exception:
                exc_info = sys.exc_info()
                # kwargs doesn't match a variable in the message
                # log the issue and the kwargs
                LOG.exception(_LE('Exception in string format operation'))
                for name, value in six.iteritems(kwargs):
                    LOG.error("%s: %s" % (name, value))    # noqa

                if CONF.fatal_exception_format_errors:
                    six.reraise(*exc_info)
                else:
                    # at least get the core message out if something happened
                    message = self.msg_fmt

        self.message = message
        super(HyRpcException, self).__init__(message)


class NotFound(HyRpcException):
    msg_fmt = _("Resource could not be found.")
    code = 404


class Invalid(HyRpcException):
    msg_fmt = _("Unacceptable parameters.")
    code = 400


# Cannot be templated as the error syntax varies.
# msg needs to be constructed when raised.
class InvalidParameterValue(Invalid):
    msg_fmt = _("%(err)s")
