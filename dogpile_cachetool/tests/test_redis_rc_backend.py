import os

import unittest2
from dogpile.cache.region import _backend_loader

import dogpile_cachetool
from . import _fixtures

REDIS_PORT = int(os.getenv('DOGPILE_REDIS_PORT', '6379'))


def setup_module(module):
    try:
        import rc  # noqa
    except ImportError:
        raise unittest2.SkipTest(
            "Skip because redis-py-cluster is not installed.")
    dogpile_cachetool.register_backend()


class _TestRedisRCConn(object):

    @classmethod
    def _check_backend_available(cls, backend):
        client = backend.client
        client.set("x", "y")
        # on py3k it appears to return b"y"
        assert client.get("x").decode("ascii") == "y"
        client.delete("x")


class RedisRCTest(_TestRedisRCConn, _fixtures._GenericBackendTest):
    backend = 'dogpile_cachetool.redis_rc'
    config_args = {
        "arguments": {
            'hosts': {
                0: {'port': REDIS_PORT, 'db': 12},
                1: {'port': REDIS_PORT, 'db': 13},
                2: {'port': REDIS_PORT, 'db': 14},
                3: {'port': REDIS_PORT, 'db': 15},
            },
        },
    }


class RedisRCConnectionTest(unittest2.TestCase):
    backend = 'dogpile_cachetool.redis_rc'

    @classmethod
    def setUpClass(cls):
        cls.backend_cls = _backend_loader.load(cls.backend)

    def test_distributed_lock_is_not_available_for_now(self):
        arguments = {
            'hosts': {},
            'distributed_lock': True,
        }
        with self.assertRaises(NotImplementedError):
            self.backend_cls(arguments)
