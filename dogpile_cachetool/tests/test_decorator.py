# -*- coding: utf-8 -*-
import itertools
import unittest2

from dogpile_cachetool import core as cache


class DecoratorTest(unittest2.TestCase):
    def setUp(self):
        super(DecoratorTest, self).setUp()
        self.region = cache.create_region()
        self.conf = {
            'backend': 'dogpile.cache.memory',
            'proxies': ['dogpile_cachetool.testing.CacheIsolatingProxy'],
        }
        cache.configure_cache_region(self.region, self.conf)

    def _fixture(self, namespace):
        counter = itertools.count(1)

        @self.region.cache_on_arguments(namespace)
        def go(a, b):
            val = next(counter)
            return val, a, b
        return go

    def _multi_fixture(self, namespace):
        counter = itertools.count(1)

        @self.region.cache_multi_on_arguments(namespace=namespace)
        def go(*args):
            val = next(counter)
            return ["%d %s" % (val, arg) for arg in args]
        return go

    def test_decorator_namespace_bytes(self):
        go = self._fixture(namespace=b'bytes')
        self.assertEqual(go('a', u'中文'), (1, 'a', u'中文'))
        self.assertEqual(go(u'b', u'中文'), (2, u'b', u'中文'))

    def test_decorator_namespace_unicode(self):
        go = self._fixture(namespace=u'unicode')
        self.assertEqual(go('a', u'中文'), (1, 'a', u'中文'))
        self.assertEqual(go(u'b', u'中文'), (2, u'b', u'中文'))

    def test_decorator_multi_namespace_bytes(self):
        go = self._multi_fixture(namespace=b'bytes')
        self.assertEqual(go('a', u'中文'), ['1 a', u'1 中文'])
        self.assertEqual(go(u'b', u'中文'), [u'2 b', u'1 中文'])

    def test_decorator_multi_namespace_unicode(self):
        go = self._multi_fixture(namespace=u'unicode')
        self.assertEqual(go('a', u'中文'), ['1 a', u'1 中文'])
        self.assertEqual(go(u'b', u'中文'), [u'2 b', u'1 中文'])
