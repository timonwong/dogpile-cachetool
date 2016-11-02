"""Items useful for external testing."""
import copy

from dogpile.cache import proxy

from dogpile_cachetool import core as cache


__all__ = [
    'CacheIsolatingProxy',
]


NO_VALUE = cache.NO_VALUE


def _copy_value(value):
    if value is not NO_VALUE:
        value = copy.deepcopy(value)
    return value


class CacheIsolatingProxy(proxy.ProxyBackend):
    """Proxy that forces a memory copy of stored values.

    The default in-memory cache-region does not perform a copy on values it
    is meant to cache.  Therefore if the value is modified after set or after
    get, the cached value also is modified.  This proxy does a copy as the last
    thing before storing data.

    In your application's tests, you'll want to set this as a proxy for the
    in-memory cache.
    """
    def get(self, key):
        return _copy_value(self.proxied.get(key))

    def set(self, key, value):
        self.proxied.set(key, _copy_value(value))
