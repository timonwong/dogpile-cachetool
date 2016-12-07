"""Caching layer implementation.

Ideas taken from OpenStack's keystone.common.cache.core.
"""
import functools
from hashlib import sha1
import logging
import sys
import threading
import traceback

import dogpile
import dogpile.cache
from dogpile.cache import api
from dogpile.cache import util
import six

from dogpile_cachetool.backends.debug import _DebugProxy


NO_VALUE = api.NO_VALUE
_MAX_KEY_SIZE = 512
_INVALIDATED_CACHE_KEY = '_cached_invalidated'
_LOG = logging.getLogger(__name__)


def import_class(import_str):
    """Returns a class from a string including module and class."""
    mod_str, _sep, class_str = import_str.rpartition('.')
    __import__(mod_str)
    try:
        return getattr(sys.modules[mod_str], class_str)
    except AttributeError:
        raise ImportError('Class %s cannot be found (%s)' %
                          (class_str,
                           traceback.format_exception(*sys.exc_info())))


def register_backend():
    dogpile.cache.register_backend(
        'dogpile_cachetool.redis_rc',
        'dogpile_cachetool.backends.redis_rc',
        'RedisRCBackend')
    dogpile.cache.register_backend(
        'dogpile_cachetool.rediscluster',
        'dogpile_cachetool.backends.rediscluster',
        'RedisClusterBackend')


def create_key_mangler(region_name=None):
    if not region_name:
        region_name = '_'

    def _real_key_manger(key):
        if isinstance(key, six.text_type):
            key = key.encode('utf-8', errors='xmlcharrefreplace')

        return b'%s.%s' % (region_name, key)
    return _real_key_manger


def create_region(*args, **kwargs):
    """Instantiate a new :class:`.SharedExpirationCacheRegion`.

    Currently, :func:`.create_region` is a pass-through to
    :class:`.SharedExpirationCacheRegion`.  See that class for
    constructor arguments.
    """
    kwargs.setdefault('function_key_generator', function_key_generator)
    kwargs.setdefault('function_multi_key_generator',
                      function_multi_key_generator)
    return SharedExpirationCacheRegion(*args, **kwargs)


def configure_cache_region(region, conf):
    """Configure a cache region."""
    if not isinstance(region, dogpile.cache.CacheRegion):
        raise TypeError('region not type dogpile.cache.CacheRegion')

    if not region.is_configured:
        region.configure(conf['backend'],
                         expiration_time=conf.get('expiration_time'),
                         arguments=conf.get('arguments'))

        if conf.get('debug'):
            region.wrap(_DebugProxy)

        if region.key_mangler is None:
            region.key_mangler = _mangle_key

        for class_path in conf.get('proxies') or []:
            proxy_class = import_class(class_path)
            _LOG.debug("Adding proxy backend to cache region: %s.", class_path)
            region.wrap(proxy_class)

    return region


def _mangle_key(key):
    try:
        key = key.encode('utf-8', errors='xmlcharrefreplace')
    except (UnicodeError, AttributeError):
        pass

    if len(key) > _MAX_KEY_SIZE:
        hashed = sha1(key).hexdigest().encode('utf-8')
        l = _MAX_KEY_SIZE - len(hashed) - 1
        key = b'%s-%s' % (key[:l], hashed)

    return key


def _key_generate_to_str(s):
    try:
        return str(s)
    except UnicodeEncodeError:
        return s.encode('utf-8')


def dont_cache_none(value):
    return value is not None


def get_memoization_decorator(region, namespace=None, expiration_time=None,
                              should_cache_fn=None):
    """Build a function based on the `cache_on_arguments` decorator."""
    memoize = region.cache_on_arguments(namespace=namespace,
                                        should_cache_fn=should_cache_fn,
                                        expiration_time=expiration_time)

    # Make sure the actual "should_cache" and "expiration_time" methods are
    # available. This is potentially interesting/useful to pre-seed cache
    # values.
    memoize.should_cache = should_cache_fn
    memoize.get_expiration_time = expiration_time

    return memoize


def function_key_generator(namespace, fn, **kwargs):
    namespace = _key_generate_to_str(namespace)
    return util.function_key_generator(namespace, fn,
                                       to_str=_key_generate_to_str)


def function_multi_key_generator(namespace, fn, **kwargs):
    namespace = _key_generate_to_str(namespace)
    return util.function_multi_key_generator(namespace, fn,
                                             to_str=_key_generate_to_str)


def _with_invalidation_cache(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        try:
            setattr(self._thread_local, _INVALIDATED_CACHE_KEY, {})
            return f(self, *args, **kwargs)
        finally:
            try:
                delattr(self._thread_local, _INVALIDATED_CACHE_KEY)
            except AttributeError:
                pass

    return wrapper


class _Invalidated(object):
    """Hack which replace dogpile internal "invalidated" property.

    TODO(timonwong): Can be better when this change is shipped:
        https://bitbucket.org/zzzeek/dogpile.cache/issues/38
    """
    _REGION_KEY = '_RegionExpiration.%(type)s.%(region_name)s'

    def __init__(self, invalidate_type):
        self.invalidate_type = invalidate_type

    def _get_region_key(self, region):
        region_key = self._REGION_KEY % {
            'type': self.invalidate_type,
            'region_name': region.name
        }
        return region_key

    def _getter_func(self, region):
        # it is required to call backend directly, because region.get
        # checks _soft/_hard_invalidated and it causes a recursion.
        # since region.get is bypassed, keys need to be hashed here.
        key = self._get_region_key(region)
        if region.key_mangler:
            key = region.key_mangler(key)
        invalidated = region.backend.get(key)
        if invalidated is not api.NO_VALUE:
            return invalidated.payload
        return None

    def __get__(self, region, objtype=None):
        if not region:
            return self
        if not region.is_configured:
            return None

        # Make some efforts to reduce actual calls to the backend when fetching
        # the "invalidated" state (both "soft" and "hard").
        state = getattr(region._thread_local, _INVALIDATED_CACHE_KEY, None)
        if state is None:
            return self._getter_func(region)

        typ = self.invalidate_type
        if typ not in state:
            state[typ] = self._getter_func(region)
        return state[typ]

    def __set__(self, region, value):
        if not region.is_configured:
            return
        key = self._get_region_key(region)
        region.set(key, value)

    def __delete__(self, region):
        if not region.is_configured:
            return
        key = self._get_region_key(region)
        region.delete(key)


class SharedExpirationCacheRegion(dogpile.cache.CacheRegion):
    """Patch the region interfaces to ensure we share the expiration time."""

    _hard_invalidated = _Invalidated('hard')
    _soft_invalidated = _Invalidated('soft')

    def __init__(self, *args, **kwargs):
        """Construct a new :class:`.SharedExpirationCacheRegion`."""
        self._thread_local = threading.local()
        super(SharedExpirationCacheRegion, self).__init__(*args, **kwargs)

    __init__.__doc__ = dogpile.cache.CacheRegion.__init__.__doc__

    @_with_invalidation_cache
    def get_or_create(self, *args, **kwargs):
        return super(SharedExpirationCacheRegion, self).get_or_create(
            *args, **kwargs)

    @_with_invalidation_cache
    def get_or_create_multi(self, *args, **kwargs):
        return super(SharedExpirationCacheRegion, self).get_or_create_multi(
            *args, **kwargs)
