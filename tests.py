#!/usr/bin/env python

import glob
import os
import sys
import time
import unittest

from ucache import *


class BaseTestCache(object):
    cache_files = []

    def get_cache(self, **kwargs):
        raise NotImplementedError

    def cleanup(self):
        for filename in self.cache_files:
            if os.path.exists(filename):
                os.unlink(filename)

    def setUp(self):
        self.cache = self.get_cache()
        super(BaseTestCache, self).setUp()

    def tearDown(self):
        self.cache.set_prefix()
        self.cache.close()
        self.cleanup()
        super(BaseTestCache, self).tearDown()

    def test_operations(self):
        test_data = (
            ('k1', 'v1'),
            ('k2', 2),
            ('k3', None),
            ('k4', [0, '1', [2]]),
            ('k5', {'6': ['7', 8, {'9': '10', '11': 12}]}),
        )
        test_data_dict = dict(test_data)

        for key, value in test_data:
            self.cache.set(key, value, 60)

        for key, value in test_data:
            self.assertEqual(self.cache.get(key), value)

        self.cache.delete('k1')
        self.cache.delete('k3')
        self.cache.delete('k5')

        for key in ('k1', 'k3', 'k5'):
            self.assertIsNone(self.cache.get(key))

        for key in ('k2', 'k4'):
            self.assertEqual(self.cache.get(key), test_data_dict[key])

        self.cache.flush()
        self.assertIsNone(self.cache.get('k2'))
        self.assertIsNone(self.cache.get('k4'))

    def test_bulk_operations(self):
        test_data = {
            'k1': 'v1',
            'k2': 2,
            'k3': [0, '1', [2]]}

        # Do simple bulk-set.
        self.cache.set_many(test_data, timeout=60)

        # Do single-set to ensure compatible with bulk-get.
        self.cache.set('k4', 'v4')

        # Compare results of bulk-get.
        self.assertEqual(self.cache.get_many(['k1', 'k2', 'k3', 'k4']), {
            'k1': 'v1',
            'k2': 2,
            'k3': [0, '1', [2]],
            'k4': 'v4'})

        # Do individual gets to ensure methods are compatible.
        self.assertEqual(self.cache.get('k1'), test_data['k1'])
        self.assertEqual(self.cache.get('k3'), test_data['k3'])

        # Do bulk-delete.
        self.cache.delete_many(['k1', 'k3', 'kx'])
        self.assertTrue(self.cache['k1'] is None)
        self.assertTrue(self.cache['k2'] is not None)
        self.assertTrue(self.cache['k3'] is None)

        self.assertEqual(self.cache.get_many(['k1', 'k2', 'k3']), {'k2': 2})

        # Do single-delete to ensure compatibility.
        self.cache.delete('k2')
        self.assertTrue(self.cache['k2'] is None)

    def test_preload(self):
        self.cache.set_many({'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}, timeout=60)
        self.assertEqual(self.cache.get('k1'), 'v1')
        self.assertTrue(self.cache.get('kx') is None)

        with self.cache.preload(['k1', 'k3']):
            self.assertEqual(self.cache.get('k1'), 'v1')
            self.assertEqual(self.cache.get('k3'), 'v3')
            self.assertTrue(self.cache.get('kx') is None)

            self.cache._preload['kx'] = 'preloaded'
            self.assertEqual(self.cache.get('kx'), 'preloaded')

        self.assertEqual(self.cache.get('k1'), 'v1')
        self.assertEqual(self.cache.get('k2'), 'v2')
        self.assertEqual(self.cache.get('k3'), 'v3')
        self.assertTrue(self.cache.get('kx') is None)

    def test_decorator(self):
        @self.cache.cached(10)
        def fn(seed=None):
            return time.time()

        value = fn()
        time.sleep(0.001)
        self.assertEqual(fn(), value)

        fn.bust()
        self.assertFalse(fn() == value)
        self.assertEqual(fn(), fn())
        self.assertFalse(fn(1) == fn(2))
        self.assertEqual(fn(2), fn(2))

    def test_property(self):
        class Dummy(object):
            @self.cache.cached_property
            def fn(self):
                return time.time()

        d = Dummy()
        value = d.fn
        time.sleep(0.001)
        self.assertEqual(d.fn, value)

    def test_compression(self):
        self.cache.close()
        self.cleanup()
        cache = self.get_cache(compression=True)
        data = {'k1': 'a' * 1024, 'k2': 'b' * 512, 'k3': 'c' * 200}
        cache.set_many(data, timeout=60)
        cache.set('k4', 'd' * 1024, timeout=60)

        self.assertEqual(cache.get('k4'), 'd' * 1024)
        res = cache.get_many(['k1', 'k2', 'k3'])
        self.assertEqual(res, data)
        cache.delete_many(['k1', 'k2', 'k3', 'k4'])

    def test_read_expired(self):
        self.cache.set('k1', 'v1', -1)
        self.assertTrue(self.cache.get('k1') is None)

    def test_clean_expired(self):
        if not self.cache.manual_expire:
            return

        day = 86400
        for i in range(1, 7):
            self.cache.set('k%s' % i, 'v%s' % i, (-i * day) - 1)

        self.cache.set('ka', 'va', -5)
        self.cache.set('kb', 'vb', 60)
        self.cache.set('kc', 'vc', day)

        # k1, -1 days ... k6, -6 days.
        self.assertTrue(self.cache.get('k4') is None)  # k4 is also deleted.
        self.assertEqual(self.cache.clean_expired(3), 3)  # k3, k5, k6.
        self.assertEqual(self.cache.clean_expired(3), 0)

        self.assertEqual(self.cache.clean_expired(1), 2)  # k1, k2.
        self.assertEqual(self.cache.clean_expired(), 1)  # ka.
        self.assertEqual(self.cache.clean_expired(), 0)

        # Cannot retrieve any of the expired data.
        for i in range(1, 7):
            self.assertTrue(self.cache.get('k%s' % i) is None)

        # Set some new expired keys and values.
        for i in range(3):
            self.cache.set('k%s' % i, 'v%s' % i, -3)

        self.assertTrue(self.cache.get('k1') is None)
        self.assertEqual(self.cache.clean_expired(), 2)
        self.assertEqual(self.cache.clean_expired(), 0)

        # Set expired key to a valid time.
        self.cache.set('k1', 'v1', 60)
        self.assertEqual(self.cache.get('k1'), 'v1')

        # Our original keys are still present.
        self.assertEqual(self.cache.get('kb'), 'vb')
        self.assertEqual(self.cache.get('kc'), 'vc')

    def test_prefix_and_flush(self):
        self.cache.set_prefix('a')
        self.cache.set('k0', 'v0-1')
        self.cache.set('k1', 'v1-1')

        self.cache.set_prefix('b')
        self.cache.set('k0', 'v0-2')

        # Check that keys and values are isolated properly by prefix.
        self.cache.set_prefix('a')
        self.assertEqual(self.cache.get('k0'), 'v0-1')

        self.cache.set_prefix('b')
        self.assertEqual(self.cache.get('k0'), 'v0-2')

        self.cache.set_prefix('a')
        try:
            self.cache.flush()
        except NotImplementedError:
            # Memcached does not support prefix match, so we skip.
            return

        self.assertTrue(self.cache.get('k0') is None)
        self.assertTrue(self.cache.get('k1') is None)

        self.cache.set_prefix('b')
        self.assertEqual(self.cache.get('k0'), 'v0-2')
        self.assertTrue(self.cache.get('k1') is None)


class TestKTCache(BaseTestCache, unittest.TestCase):
    def cleanup(self):
        self.cache.close(close_all=True)

    def get_cache(self, **kwargs):
        return KTCache(connection_pool=False, **kwargs)


class TestSqliteCache(BaseTestCache, unittest.TestCase):
    cache_files = ['sqlite_cache.db']

    def get_cache(self, **kwargs):
        return SqliteCache('sqlite_cache.db', **kwargs)


class TestRedisCache(BaseTestCache, unittest.TestCase):
    def get_cache(self, **kwargs):
        return RedisCache(**kwargs)

    def test_read_expired(self):
        # Redis doesn't support setting a negative timeout.
        pass


class TestKCCache(BaseTestCache, unittest.TestCase):
    def get_cache(self, **kwargs):
        return KCCache(filename='*', **kwargs)


class TestMemcacheCache(BaseTestCache, unittest.TestCase):
    def get_cache(self, **kwargs):
        return MemcacheCache(**kwargs)


class TestPyMemcacheCache(BaseTestCache, unittest.TestCase):
    def get_cache(self, **kwargs):
        return PyMemcacheCache(**kwargs)


class TestMemoryCache(BaseTestCache, unittest.TestCase):
    def get_cache(self, **kwargs):
        return MemoryCache(**kwargs)


class TestDbmCache(BaseTestCache, unittest.TestCase):
    @property
    def cache_files(self):
        return glob.glob('dbmcache.*')

    def get_cache(self, **kwargs):
        return DbmCache('dbmcache.db', **kwargs)


class TestGreenDBCache(BaseTestCache, unittest.TestCase):
    def get_cache(self, **kwargs):
        return GreenDBCache(**kwargs)


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
