import os

import unittest2

import dogpile_cachetool
from . import _fixtures


def setup_module(module):
    try:
        import rediscluster  # noqa
    except ImportError:
        raise unittest2.SkipTest(
            "Skip because redis-py-cluster is not installed.")
    dogpile_cachetool.register_backend()


class _TestRedisClusterConn(object):

    @classmethod
    def _check_backend_available(cls, backend):
        client = backend.client
        client.set("x", "y")
        # on py3k it appears to return b"y"
        assert client.get("x").decode("ascii") == "y"
        client.delete("x")


@unittest2.skipIf(
    os.environ.get("CACHETOOL_BACKEND") not in (None, "rediscluster"),
    "backend test skip")
class RedisClusterTest(_TestRedisClusterConn, _fixtures._GenericBackendTest):
    backend = 'dogpile_cachetool.rediscluster'
    config_args = {
        "arguments": {
            'host': '127.0.0.1',
            'port': 7000,
        },
    }
