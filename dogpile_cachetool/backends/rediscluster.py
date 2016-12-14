from __future__ import absolute_import

from dogpile.cache.backends.redis import RedisBackend

from dogpile_cachetool.utils import cached_property


class RedisClusterBackend(RedisBackend):
    """A `RedisCluster <http://redis.io/>`_ backend, using the
    `redis-py-cluster <http://pypi.python.org/pypi/redis-py-cluster/>`_
    backend.

    Example configuration::

        from dogpile.cache import make_region

        region = make_region().configure(
            'dogpile_cachetool.rediscluster',
            arguments = {
                'host': 'localhost',
                'port': 6379,
                'db': 0,
                'redis_expiration_time': 60*60*2,   # 2 hours
                'distributed_lock': True,
                'skip_full_coverage_check': True,
            }
        )

    Arguments accepted in the arguments dictionary:

    :param url: string. If provided, will override separate host/port/db
     params.  The format is that accepted by ``StrictRedis.from_url()``.

    :param host: string, default is ``localhost``.

    :param password: string, default is no password.

    :param port: integer, default is ``6379``.

    :param db: integer, default is ``0``.

    :param redis_expiration_time: integer, number of seconds after setting
     a value that Redis should expire it.  This should be larger than dogpile's
     cache expiration.  By default no expiration is set.

    :param distributed_lock: boolean, when True, will use a
     redis-lock as the dogpile lock.
     Use this when multiple
     processes will be talking to the same redis instance.
     When left at False, dogpile will coordinate on a regular
     threading mutex.

    :param lock_timeout: integer, number of seconds after acquiring a lock that
     Redis should expire it.  This argument is only valid when
     ``distributed_lock`` is ``True``.

    :param socket_timeout: float, seconds for socket timeout.
     Default is None (no timeout).

    :param lock_sleep: integer, number of seconds to sleep when failed to
     acquire a lock.  This argument is only valid when
     ``distributed_lock`` is ``True``.

    :param connection_pool: ``redis.ConnectionPool`` object.  If provided,
     this object supersedes other connection arguments passed to the
     ``redis.StrictRedis`` instance, including url and/or host as well as
     socket_timeout, and will be passed to ``redis.StrictRedis`` as the
     source of connectivity.

    :param max_connections: Maximum number of connections that should be kept
    open at one time

    :param skip_full_coverage_check: Skips the check of
        cluster-require-full-coverage config, useful for clusters without
        the CONFIG command (like aws)

    """

    # noinspection PyMissingConstructor
    def __init__(self, arguments):
        arguments = arguments.copy()
        self.url = arguments.pop('url', None)
        self.host = arguments.pop('host', 'localhost')
        self.password = arguments.pop('password', None)
        self.port = arguments.pop('port', 6379)
        self.db = arguments.pop('db', 0)
        self.distributed_lock = arguments.get('distributed_lock', False)
        self.socket_timeout = arguments.pop('socket_timeout', None)

        self.lock_timeout = arguments.get('lock_timeout', None)
        self.lock_sleep = arguments.get('lock_sleep', 0.1)

        self.redis_expiration_time = arguments.pop('redis_expiration_time', 0)
        self.max_connections = arguments.pop('max_connections', 32)
        self.skip_full_coverage_check = arguments.pop(
            'skip_full_coverage_check', False)
        self.connection_pool = arguments.get('connection_pool', None)
        self.client = self._client

    @cached_property
    def _client(self):
        import rediscluster

        if self.connection_pool is not None:
            # the connection pool already has all other connection
            # options present within, so here we disregard socket_timeout
            # and others.
            return rediscluster.StrictRedisCluster(
                connection_pool=self.connection_pool)

        args = {
            'skip_full_coverage_check': self.skip_full_coverage_check,
            'max_connections': self.max_connections,
        }
        if self.socket_timeout:
            args['socket_timeout'] = self.socket_timeout
        if self.url is not None:
            args.update(url=self.url)
        else:
            args.update(
                host=self.host,
                password=self.password,
                port=self.port,
            )
        return rediscluster.StrictRedisCluster(**args)
