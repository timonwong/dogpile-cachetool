# -*- coding: utf-8 -*-
import copy
import unittest2

from dogpile.cache import proxy

from dogpile_cachetool import core as cache


NO_VALUE = cache.NO_VALUE


def _copy_value(value):
    if value is not NO_VALUE:
        value = copy.deepcopy(value)
    return value


class TestProxy(proxy.ProxyBackend):
    def get(self, key):
        value = _copy_value(self.proxied.get(key))
        if value is not NO_VALUE:
            if isinstance(value[0], TestProxyValue):
                value[0].cached = True
        return value


class TestProxyValue(object):
    def __init__(self, value):
        self.value = value
        self.cached = False


class UTF8KeyManglerTests(unittest2.TestCase):

    def test_key_is_utf8_encoded(self):
        key = u'fäké1'
        encoded = cache._mangle_key(key)
        self.assertIsNotNone(encoded)

    def test_key_is_bytestring(self):
        key = b'\xcf\x84o\xcf\x81\xce\xbdo\xcf\x82'
        encoded = cache._mangle_key(key)
        self.assertIsNotNone(encoded)

    def test_key_is_string(self):
        key = 'fake'
        encoded = cache._mangle_key(key)
        self.assertIsNotNone(encoded)

    def test_long_key(self):
        key = u'fake' * 256
        encoded = cache._mangle_key(key)
        self.assertEqual(len(encoded), cache._MAX_KEY_SIZE)


class CacheRegionTest(unittest2.TestCase):
    def setUp(self):
        super(CacheRegionTest, self).setUp()
        self.region = cache.create_region()
        self.conf = {
            'backend': 'dogpile.cache.memory',
            'proxies': ['dogpile_cachetool.testing.CacheIsolatingProxy'],
        }
        cache.configure_cache_region(self.region, self.conf)
        self.region.wrap(TestProxy)
        self.test_value = TestProxyValue('Decorator Test')

    def test_region_built_with_proxy_direct_cache_test(self):
        # Verify cache regions are properly built with proxies.
        test_value = TestProxyValue('Direct Cache Test')
        self.region.set('cache_test', test_value)
        cached_value = self.region.get('cache_test')
        self.assertTrue(cached_value.cached)

    def test_cache_region_no_error_multiple_config(self):
        # Verify configuring the CacheRegion again doesn't error.
        cache.configure_cache_region(self.region, self.conf)
        cache.configure_cache_region(self.region, self.conf)

    def test_cache_debug_proxy(self):
        single_value = 'Test Value'
        single_key = 'testkey'
        multi_values = {'key1': 1, 'key2': 2, 'key3': 3}

        self.region.set(single_key, single_value)
        self.assertEqual(single_value, self.region.get(single_key))

        self.region.delete(single_key)
        self.assertEqual(NO_VALUE, self.region.get(single_key))

        self.region.set_multi(multi_values)
        cached_values = self.region.get_multi(multi_values.keys())
        for value in multi_values.values():
            self.assertIn(value, cached_values)
        self.assertEqual(len(multi_values.values()), len(cached_values))

        self.region.delete_multi(multi_values.keys())
        for value in self.region.get_multi(multi_values.keys()):
            self.assertEqual(NO_VALUE, value)

    def test_configure_non_region_object_raises_error(self):
        self.assertRaises(TypeError, cache.configure_cache_region,
                          "xxxxxxx", {})
