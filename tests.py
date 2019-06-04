#!/usr/bin/env python

import glob
import os
import sys
import time
import unittest

from ucache import *


class BaseTestCache(object):
    cache_files = []

    def get_cache(self, compression=False):
        raise NotImplementedError

    def cleanup(self):
        for filename in self.cache_files:
            if os.path.exists(filename):
                os.unlink(filename)

    def setUp(self):
        self.cache = self.get_cache()
        super(BaseTestCache, self).setUp()

    def tearDown(self):
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


class TestKTCache(BaseTestCache, unittest.TestCase):
    def get_cache(self, compression=False):
        return KTCache(connection_pool=False, compression=compression)


class TestSqliteCache(BaseTestCache, unittest.TestCase):
    cache_files = ['sqlite_cache.db']

    def get_cache(self, compression=False):
        return SqliteCache('sqlite_cache.db', compression=compression)


class TestRedisCache(BaseTestCache, unittest.TestCase):
    def get_cache(self, compression=False):
        return RedisCache(compression=compression)


class TestKCCache(BaseTestCache, unittest.TestCase):
    def get_cache(self, compression=False):
        return KCCache(filename='*', compression=compression)


class TestMemcacheCache(BaseTestCache, unittest.TestCase):
    def get_cache(self, compression=False):
        return MemcacheCache(compression=compression)


class TestPyMemcacheCache(BaseTestCache, unittest.TestCase):
    def get_cache(self, compression=False):
        return PyMemcacheCache(compression=compression)


class TestMemoryCache(BaseTestCache, unittest.TestCase):
    def get_cache(self, compression=False):
        return MemoryCache(compression=compression)


class TestDbmCache(BaseTestCache, unittest.TestCase):
    @property
    def cache_files(self):
        return glob.glob('dbmcache.*')

    def get_cache(self, compression=False):
        return DbmCache('dbmcache.db', compression=compression)


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
