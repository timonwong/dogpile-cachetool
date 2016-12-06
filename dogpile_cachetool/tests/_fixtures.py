import collections
import itertools
import os
import random
import time
import unittest2
from binascii import hexlify
from threading import Lock
from threading import Thread

from dogpile.cache import CacheRegion
from dogpile.cache.api import NO_VALUE
from dogpile.cache.region import _backend_loader


class _GenericBackendFixture(object):
    @classmethod
    def setUpClass(cls):
        super(_GenericBackendFixture, cls).setUpClass()
        backend_cls = _backend_loader.load(cls.backend)
        arguments = cls.config_args.get('arguments', {})
        backend = backend_cls(arguments)
        cls._check_backend_available(backend)

    def setUp(self):
        self._random_some_key = "some key:%s" % hexlify(os.urandom(8))
        self._random_backend_key = "some_key:%s" % hexlify(os.urandom(8))

    def tearDown(self):
        if self._region_inst:
            for key in self._keys:
                self._region_inst.delete(key)
            self._keys.clear()
        elif self._backend_inst:
            self._backend_inst.delete(self._random_backend_key)

    @classmethod
    def _check_backend_available(cls, backend):
        pass

    region_args = {}
    config_args = {}

    _region_inst = None
    _backend_inst = None

    _keys = set()

    def _region(self, backend=None, region_args=None, config_args=None):
        if region_args is None:
            region_args = {}
        if config_args is None:
            config_args = {}

        _region_args = self.region_args.copy()
        _region_args.update(**region_args)
        _config_args = self.config_args.copy()
        _config_args.update(config_args)

        def _store_keys(key):
            if existing_key_mangler:
                key = existing_key_mangler(key)
            self._keys.add(key)
            return key

        reg = CacheRegion(**_region_args)
        self._region_inst = reg

        existing_key_mangler = self._region_inst.key_mangler
        reg._user_defined_key_mangler = _store_keys
        reg.key_mangler = _store_keys
        reg.configure(backend or self.backend, **_config_args)
        return reg

    def _backend(self):
        backend_cls = _backend_loader.load(self.backend)
        _config_args = self.config_args.copy()
        arguments = _config_args.get('arguments', {})
        self._backend_inst = backend_cls(arguments)
        return self._backend_inst


class _GenericBackendTest(_GenericBackendFixture, unittest2.TestCase):

    def test_backend_get_nothing(self):
        backend = self._backend()
        self.assertEqual(backend.get(self._random_backend_key), NO_VALUE)

    def test_backend_delete_nothing(self):
        backend = self._backend()
        backend.delete(self._random_backend_key)

    def test_backend_set_get_value(self):
        backend = self._backend()
        backend.set(self._random_backend_key, "some value")
        self.assertEqual(backend.get(self._random_backend_key), "some value")

    def test_backend_delete(self):
        backend = self._backend()
        backend.set(self._random_backend_key, "some value")
        backend.delete(self._random_backend_key)
        self.assertEqual(backend.get(self._random_backend_key), NO_VALUE)

    def test_region_set_get_value(self):
        reg = self._region()
        reg.set(self._random_some_key, "some value")
        self.assertEqual(reg.get(self._random_some_key), "some value")

    def test_region_set_multiple_values(self):
        reg = self._region()
        n = 50
        values = dict(
            ('key%d' % i, 'value%d' % i)
            for i in range(1, n + 1)
        )
        reg.set_multi(values)
        for i in range(1, n + 1):
            k = 'key%d' % i
            v = 'value%d' % i
            self.assertEqual(reg.get(k), v)

    def test_region_get_zero_multiple_values(self):
        reg = self._region()
        self.assertEqual(reg.get_multi([]), [])

    def test_region_set_zero_multiple_values(self):
        reg = self._region()
        reg.set_multi({})

    def test_region_set_zero_multiple_values_w_decorator(self):
        reg = self._region()
        values = reg.get_or_create_multi([], lambda: 0)
        self.assertEqual(values, [])

    def test_region_get_multiple_values(self):
        reg = self._region()
        key1 = 'value1'
        key2 = 'value2'
        key3 = 'value3'
        reg.set('key1', key1)
        reg.set('key2', key2)
        reg.set('key3', key3)
        values = reg.get_multi(['key1', 'key2', 'key3'])
        self.assertEqual(
            [key1, key2, key3], values
        )

    def test_region_get_nothing_multiple(self):
        reg = self._region()
        reg.delete_multi(['key1', 'key2', 'key3', 'key4', 'key5'])
        values = {'key1': 'value1', 'key3': 'value3', 'key5': 'value5'}
        reg.set_multi(values)
        reg_values = reg.get_multi(
            ['key1', 'key2', 'key3', 'key4', 'key5', 'key6'])
        self.assertEqual(
            reg_values,
            ["value1", NO_VALUE, "value3", NO_VALUE,
                "value5", NO_VALUE
             ]
        )

    def test_region_get_empty_multiple(self):
        reg = self._region()
        reg_values = reg.get_multi([])
        self.assertEqual(reg_values, [])

    def test_region_delete_multiple(self):
        reg = self._region()
        values = {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}
        reg.set_multi(values)
        reg.delete_multi(['key2', 'key10'])
        self.assertEqual(values['key1'], reg.get('key1'))
        self.assertEqual(NO_VALUE, reg.get('key2'))
        self.assertEqual(values['key3'], reg.get('key3'))
        self.assertEqual(NO_VALUE, reg.get('key10'))

    def test_region_set_get_nothing(self):
        reg = self._region()
        reg.delete_multi([self._random_some_key])
        self.assertEqual(reg.get(self._random_some_key), NO_VALUE)

    def test_region_creator(self):
        reg = self._region()

        def creator():
            return "some value"
        self.assertEqual(
            reg.get_or_create(self._random_some_key, creator), "some value")

    @unittest2.skipIf(os.environ.get('CI') == 'true',
                      "Not needed in CI environment")
    def test_threaded_dogpile(self):
        # run a basic dogpile concurrency test.
        # note the concurrency of dogpile itself
        # is intensively tested as part of dogpile.
        reg = self._region(config_args={"expiration_time": .25})
        lock = Lock()
        canary = []

        def creator():
            ack = lock.acquire(False)
            canary.append(ack)
            time.sleep(.25)
            if ack:
                lock.release()
            return "some value"

        def f():
            for x in range(5):
                reg.get_or_create(self._random_some_key, creator)
                time.sleep(.5)

        threads = [Thread(target=f) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(canary) > 2
        if not reg.backend.has_lock_timeout():
            assert False not in canary
        else:
            assert False in canary

    @unittest2.skipIf(os.environ.get('CI') == 'true',
                      "Not needed in CI environment")
    def test_threaded_get_multi(self):
        reg = self._region(config_args={"expiration_time": .25})
        locks = dict((str(i), Lock()) for i in range(11))

        canary = collections.defaultdict(list)

        def creator(*keys):
            assert keys
            ack = [locks[key].acquire(False) for key in keys]

            for acq, key in zip(ack, keys):
                canary[key].append(acq)

            time.sleep(.5)

            for acq, key in zip(ack, keys):
                if acq:
                    locks[key].release()
            return ["some value %s" % k for k in keys]

        def f():
            for x in range(5):
                reg.get_or_create_multi(
                    [str(random.randint(1, 10))
                        for i in range(random.randint(1, 5))],
                    creator)
                time.sleep(.5)
        f()

    def test_region_delete(self):
        reg = self._region()
        reg.set(self._random_some_key, "some value")
        reg.delete(self._random_some_key)
        reg.delete(self._random_some_key)
        self.assertEqual(reg.get(self._random_some_key), NO_VALUE)

    def test_region_expire(self):
        reg = self._region(config_args={"expiration_time": .25})
        counter = itertools.count(1)

        def creator():
            return "some value %d" % next(counter)
        self.assertEqual(
            reg.get_or_create(self._random_some_key, creator), "some value 1")
        time.sleep(.4)
        self.assertEqual(
            reg.get(self._random_some_key, ignore_expiration=True),
            "some value 1")
        self.assertEqual(reg.get_or_create(self._random_some_key, creator),
                         "some value 2")
        self.assertEqual(reg.get(self._random_some_key), "some value 2")

    def test_decorated_fn_functionality(self):
        # test for any quirks in the fn decoration that interact
        # with the backend.

        reg = self._region()

        counter = itertools.count(1)

        @reg.cache_on_arguments()
        def my_function(x, y):
            return next(counter) + x + y

        # Start with a clean slate
        my_function.invalidate(3, 4)
        my_function.invalidate(5, 6)
        my_function.invalidate(4, 3)

        self.assertEqual(my_function(3, 4), 8)
        self.assertEqual(my_function(5, 6), 13)
        self.assertEqual(my_function(3, 4), 8)
        self.assertEqual(my_function(4, 3), 10)

        my_function.invalidate(4, 3)
        self.assertEqual(my_function(4, 3), 11)

    def test_exploding_value_fn(self):
        reg = self._region()

        def boom():
            raise Exception("boom")

        with self.assertRaisesRegexp(Exception, r'boom'):
            reg.get_or_create(self._random_backend_key, boom)
