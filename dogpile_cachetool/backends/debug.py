import logging

from dogpile.cache import proxy


_LOG = logging.getLogger(__name__)


class _DebugProxy(proxy.ProxyBackend):
    """Extra Logging ProxyBackend."""
    # NOTE(morganfainberg): Pass all key/values through repr to ensure we have
    # a clean description of the information.  Without use of repr, it might
    # be possible to run into encode/decode error(s). For logging/debugging
    # purposes encode/decode is irrelevant and we should be looking at the
    # data exactly as it stands.

    def get(self, key):
        value = self.proxied.get(key)
        _LOG.debug('CACHE_GET: Key: "%(key)r" Value: "%(value)r"',
                   {'key': key, 'value': value})
        return value

    def get_multi(self, keys):
        values = self.proxied.get_multi(keys)
        _LOG.debug('CACHE_GET_MULTI: "%(keys)r" Values: "%(values)r"',
                   {'keys': keys, 'values': values})
        return values

    def set(self, key, value):
        _LOG.debug('CACHE_SET: Key: "%(key)r" Value: "%(value)r"',
                   {'key': key, 'value': value})
        return self.proxied.set(key, value)

    def set_multi(self, keys):
        _LOG.debug('CACHE_SET_MULTI: "%r"', keys)
        self.proxied.set_multi(keys)

    def delete(self, key):
        self.proxied.delete(key)
        _LOG.debug('CACHE_DELETE: "%r"', key)

    def delete_multi(self, keys):
        _LOG.debug('CACHE_DELETE_MULTI: "%r"', keys)
        self.proxied.delete_multi(keys)
