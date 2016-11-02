from dogpile.cache import api
import rc
from rc.cache import cached_property
import rc.redis_clients as rc_clients
import rc.redis_cluster as rc_cluster
from six import u
from six.moves import cPickle as pickle


class _ClusterClient(rc_clients.RedisClusterClient):
    def mset(self, mapping):
        commands = []
        for name, value in mapping.items():
            commands.append(('SET', name, value))
        results = self._execute_multi_command_with_poller('SET',
                                                          commands)
        return all(results.values())


class _Cluster(rc_cluster.RedisCluster):
    def get_client(self, max_concurrency=64, poller_timeout=1.0):
        return _ClusterClient(
            rc_cluster.RedisClusterPool(self), max_concurrency,
            poller_timeout)


class RedisRCBackend(api.CacheBackend):
    """"A `Redis <http://redis.io/>`_ backend, using the
    `rc <https://pypi.python.org/pypi/rc/>`_ backend.

    Example configuration::

        from dogpile.cache import make_region

        region = make_region().configure(
            'dogpile.cache.redis_rc',
            arguments = {
                'hosts': {
                    0: {'port': 6379},
                    1: {'port': 6479},
                    2: {'port': 6579},
                    3: {'port': 6679},
                },
                'redis_expiration_time': 60*60*2,   # 2 hours
                'distributed_lock': False,
            }
        )
    """

    # noinspection PyMissingConstructor
    def __init__(self, arguments):
        arguments = arguments.copy()

        self.hosts = arguments['hosts']
        self.distributed_lock = arguments.get('distributed_lock', False)
        if self.distributed_lock:
            raise NotImplementedError(
                "distributed_lock is not currently supported yet")

        self.lock_timeout = arguments.get('lock_timeout', None)
        self.lock_sleep = arguments.get('lock_sleep', 0.1)

        self.redis_expiration_time = arguments.pop('redis_expiration_time', 0)
        self.max_concurrency = arguments.pop('max_concurrency', 64)
        self.poller_timeout = arguments.pop('poller_timeout', 1.0)

    @cached_property
    def client(self):
        cluster = _Cluster(
            self.hosts,
            router_cls=rc.RedisConsistentHashRouter,
            router_options=None,  # Not used (deliberately)
            pool_cls=None,  # Use the default one (deliberately)
            pool_options=None,  # Not used (deliberately)
        )
        return cluster.get_client(max_concurrency=self.max_concurrency,
                                  poller_timeout=self.poller_timeout)

    def get_mutex(self, key):
        if self.distributed_lock:
            return self.client.lock(u('_lock{0}').format(key),
                                    self.lock_timeout, self.lock_sleep)
        else:
            return None

    def get(self, key):
        value = self.client.get(key)
        if value is None:
            return api.NO_VALUE
        return pickle.loads(value)

    def get_multi(self, keys):
        if not keys:
            return []
        values = self.client.mget(keys)
        return [
            pickle.loads(v) if v is not None else api.NO_VALUE
            for v in values]

    def set(self, key, value):
        if self.redis_expiration_time:
            self.client.setex(key, self.redis_expiration_time,
                              pickle.dumps(value, pickle.HIGHEST_PROTOCOL))
        else:
            self.client.set(key, pickle.dumps(value, pickle.HIGHEST_PROTOCOL))

    def set_multi(self, mapping):
        mapping = dict(
            (k, pickle.dumps(v, pickle.HIGHEST_PROTOCOL))
            for k, v in mapping.items()
        )

        if not self.redis_expiration_time:
            self.client.mset(mapping)
        else:
            self.client.msetex(mapping, self.redis_expiration_time)

    def delete(self, key):
        self.client.delete(key)

    def delete_multi(self, keys):
        self.client.mdelete(*keys)
