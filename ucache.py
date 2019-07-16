from collections import ChainMap
from collections import Counter
from contextlib import contextmanager
import atexit
import functools
import hashlib
import pickle
import struct
import threading
import time
import zlib
try:
    import msgpack
except ImportError:
    msgpack = None


__version__ = '0.1.4'
__all__ = [
    'DbmCache',
    'GreenDBCache',
    'KCCache',
    'KTCache',
    'MemcacheCache',
    'MemoryCache',
    'PyMemcacheCache',
    'RedisCache',
    'SqliteCache',
    'UC_NONE',
    'UC_MSGPACK',
    'UC_PICKLE',
]


class UCacheException(Exception): pass
class ImproperlyConfigured(UCacheException): pass


pdumps = lambda o: pickle.dumps(o, pickle.HIGHEST_PROTOCOL)
ploads = pickle.loads

if msgpack is not None:
    mpackb = lambda o: msgpack.packb(o, use_bin_type=True)
    munpackb = lambda b: msgpack.unpackb(b, raw=False)


def with_compression(pack, unpack, threshold=256):
    """
    Given packing and unpacking routines, return new versions that
    transparently compress and decompress the serialized data.
    """
    def new_pack(value):
        flag = b'\x01'
        data = pack(value)
        if len(data) > threshold:
            flag = b'\x02'
            data = zlib.compress(data)
        return flag + data

    def new_unpack(data):
        if not data: return data
        buf = memoryview(data)  # Avoid copying data.
        flag = buf[0]
        rest = buf[1:]
        if flag == 2:
            pdata = zlib.decompress(rest)
        elif flag == 1:
            pdata = rest
        else:
            # Backwards-compatibility / safety. If the value is not prefixed,
            # just deserialize all of it.
            pdata = buf
        return unpack(pdata)

    return new_pack, new_unpack

ts_struct = struct.Struct('>Q')  # 64-bit integer, timestamp in milliseconds.

def encode_timestamp(data, ts):
    ts_buf = ts_struct.pack(int(ts * 1000))
    return ts_buf + data

def decode_timestamp(data):
    mv = memoryview(data)
    ts_msec, = ts_struct.unpack(mv[:8])
    return ts_msec / 1000., mv[8:]

__sentinel__ = object()

def chunked(it, n):
    for group in (list(g) for g in izip_longest(*[iter(it)] * n,
                                                fillvalue=__sentinel__)):
        if group[-1] is __sentinel__:
            del group[group.index(__sentinel__):]
        yield group


decode = lambda s: s.decode('utf8') if isinstance(s, bytes) else s
encode = lambda s: s.encode('utf8') if isinstance(s, str) else s


UC_NONE = 0
UC_PICKLE = 1
UC_MSGPACK = 2


class CacheStats(object):
    __slots__ = ('preload_hits', 'hits', 'misses', 'writes')
    def __init__(self):
        self.preload_hits = self.hits = self.misses = self.writes = 0

    def as_dict(self):
        return {
            'preload_hits': self.preload_hits,
            'hits': self.hits,
            'misses': self.misses,
            'writes': self.writes}


class Cache(object):
    # Storage layers that do not support expiration can still be used, in which
    # case we include the timestamp in the value and transparently handle
    # serialization/deserialization. Periodic cleanup is necessary, however.
    manual_expire = False

    def __init__(self, prefix=None, timeout=60, debug=False, compression=False,
                 compression_len=256, serializer=UC_PICKLE, connect=True,
                 **params):
        self.set_prefix(prefix)
        self.timeout = timeout
        self.debug = debug
        self.compression = compression
        self.serializer = serializer
        self.params = params
        self._preload = ChainMap()  # Stack of preload data.
        self._stats = CacheStats()

        if self.serializer == UC_NONE:
            pack = unpack = lambda o: o
        elif self.serializer == UC_PICKLE:
            pack, unpack = pdumps, ploads
        elif self.serializer == UC_MSGPACK:
            pack, unpack = mpackb, munpackb

        if compression:
            pack, unpack = with_compression(pack, unpack, compression_len)

        self.pack = pack
        self.unpack = unpack

        if connect:
            self.open()

    def set_prefix(self, prefix=None):
        self.prefix = encode(prefix or '')
        self._prefix_len = len(self.prefix)

    @contextmanager
    def preload(self, keys):
        self._preload = self._preload.new_child(self.get_many(keys))
        yield
        self._preload = self._preload.parents

    def open(self):
        pass

    def close(self):
        pass

    @property
    def stats(self):
        return self._stats.as_dict()

    def prefix_key(self, key):
        if self.prefix:
            return b'%s.%s' % (self.prefix, encode(key))
        else:
            return encode(key)

    def unprefix_key(self, key):
        if self.prefix:
            return decode(key[self._prefix_len + 1:])
        else:
            return decode(key)

    def get(self, key):
        if self.debug: return

        if len(self._preload.maps) > 1 and key in self._preload:
            self._stats.preload_hits += 1
            return self._preload[key]

        data = self._get(self.prefix_key(key))
        if data is None:
            self._stats.misses += 1
            return

        if self.manual_expire:
            ts, value = decode_timestamp(data)
            if ts >= time.time():
                self._stats.hits += 1
                return self.unpack(value)
            else:
                self._stats.misses += 1
                self.delete(key)
        else:
            self._stats.hits += 1
            return self.unpack(data)

    def _get(self, key):
        raise NotImplementedError

    def get_many(self, keys):
        if self.debug: return

        nkeys = len(keys)
        prefix_keys = [self.prefix_key(key) for key in keys]
        bulk_data = self._get_many(prefix_keys)
        accum = {}
        hits = 0

        # For tracking keys when manual_expire is true.
        timestamp = time.time()
        expired = []

        for key, data in bulk_data.items():
            if data is None: continue

            if self.manual_expire:
                ts, value = decode_timestamp(data)
                if ts >= timestamp:
                    accum[self.unprefix_key(key)] = self.unpack(value)
                    hits += 1
                else:
                    expired.append(key)
            else:
                accum[self.unprefix_key(key)] = self.unpack(data)
                hits += 1

        if expired:
            # Handle cleaning-up expired keys. Only applies to manual_expire.
            self.delete_many(expired)

        # Update stats.
        self._stats.hits += hits
        self._stats.misses += (nkeys - hits)
        return accum

    def _get_many(self, keys):
        raise NotImplementedError

    def set(self, key, value, timeout=None):
        if self.debug: return

        timeout = timeout if timeout is not None else self.timeout
        data = self.pack(value)

        if self.manual_expire:
            # Encode the expiration timestamp as the first 8 bytes of the
            # cached value.
            data = encode_timestamp(data, time.time() + timeout)

        self._stats.writes += 1
        return self._set(self.prefix_key(key), data, timeout)

    def _set(self, key, value, timeout):
        raise NotImplementedError

    def set_many(self, __data=None, timeout=None, **kwargs):
        if self.debug: return

        timeout = timeout if timeout is not None else self.timeout
        if __data is not None:
            kwargs.update(__data)

        accum = {}
        expires = time.time() + timeout

        for key, value in kwargs.items():
            data = self.pack(value)
            if self.manual_expire:
                data = encode_timestamp(data, expires)
            accum[self.prefix_key(key)] = data

        self._stats.writes += len(accum)
        return self._set_many(accum, timeout)

    def _set_many(self, data, timeout):
        raise NotImplementedError

    def delete(self, key):
        if self.debug: return
        return self._delete(self.prefix_key(key))

    def _delete(self, key):
        raise NotImplementedError

    def delete_many(self, keys):
        if self.debug: return
        return self._delete_many([self.prefix_key(key) for key in keys])

    def _delete_many(self, keys):
        raise NotImplementedError

    __getitem__ = get
    __setitem__ = set
    __delitem__ = delete

    def flush(self):
        if self.debug: return
        return self._flush()

    def _flush(self):
        raise NotImplementedError

    def clean_expired(self, ndays=0):
        n = 0
        if self.manual_expire:
            cutoff = time.time() - (ndays * 86400)
            for expired_key in self.get_expired_keys(cutoff):
                self._delete(expired_key)
                n += 1
        return n

    def get_expired_keys(self, cutoff):
        raise NotImplementedError

    def _key_fn(a, k):
        # Generic function for converting an arbitrary function call's
        # arguments (and keyword args) into a consistent hash key.
        return hashlib.md5(pdumps((a, k))).hexdigest()

    def cached(self, timeout=None, key_fn=_key_fn):
        def decorator(fn):
            def make_key(args, kwargs):
                return '%s:%s' % (fn.__name__, key_fn(args, kwargs))

            def bust(*a, **k):
                self.delete(make_key(a, k))

            @functools.wraps(fn)
            def inner(*a, **k):
                key = make_key(a, k)
                res = self.get(key)
                if res is None:
                    res = fn(*a, **k)
                    self.set(key, res, timeout)
                return res

            inner.bust = bust
            inner.make_key = make_key
            return inner
        return decorator

    def cached_property(self, timeout=None, key_fn=_key_fn):
        this = self
        class _cached_property(object):
            def __init__(self, fn):
                self._fn = this.cached(timeout=timeout, key_fn=key_fn)(fn)
            def __get__(self, instance, instance_type=None):
                if instance is None:
                    return self
                return self._fn(instance)
            def __delete__(self, obj):
                self._fn.bust(obj)
            def __set__(self, instance, value):
                raise ValueError('Cannot set value of a cached property.')
        def decorator(fn):
            return _cached_property(fn)
        return decorator


class DummyLock(object):
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class MemoryCache(Cache):
    manual_expire = True

    def __init__(self, thread_safe=True, *args, **kwargs):
        self._data = {}
        if thread_safe:
            self._lock = threading.RLock()
        else:
            self._lock = DummyLock()
        super(MemoryCache, self).__init__(*args, **kwargs)

    def _get(self, key):
        with self._lock:
            return self._data.get(key)

    def _get_many(self, keys):
        with self._lock:
            return {key: self._data[key] for key in keys if key in self._data}

    def _set(self, key, value, timeout):
        with self._lock:
            self._data[key] = value  # Ignore timeout, it is packed in value.

    def _set_many(self, data, timeout):
        with self._lock:
            self._data.update(data)

    def _delete(self, key):
        with self._lock:
            if key in self._data:
                del self._data[key]

    def _delete_many(self, keys):
        with self._lock:
            for key in keys:
                if key in self._data:
                    del self._data[key]

    def _flush(self):
        with self._lock:
            if not self.prefix:
                self._data = {}
            else:
                self._data = {k: v for k, v in self._data.items()
                              if not k.startswith(self.prefix)}
        return True

    def clean_expired(self, ndays=0):
        timestamp = time.time() - (ndays * 86400)
        n = 0

        with self._lock:
            for key, value in list(self._data.items()):
                ts, _ = decode_timestamp(value)
                if ts <= timestamp:
                    del self._data[key]
                    n += 1
        return n


try:
    from ukt import KT_NONE
    from ukt import KyotoTycoon
except ImportError:
    KyotoTycoon = None


class KTCache(Cache):
    def __init__(self, host='127.0.0.1', port=1978, db=0, client_timeout=5,
                 connection=None, no_reply=False, **params):
        if KyotoTycoon is None:
            raise ImproperlyConfigured('Cannot use KTCache - ukt python '
                                       'module is not installed.')

        self._host = host
        self._port = port
        self._db = db
        self._client_timeout = client_timeout
        self._no_reply = no_reply

        if connection is not None:
            self._client = connection
        else:
            self._client = self._get_client()
        super(KTCache, self).__init__(**params)

    def _get_client(self):
        return KyotoTycoon(host=self._host, port=self._port,
                           timeout=self._client_timeout, default_db=self._db,
                           serializer=KT_NONE)

    def close(self, close_all=False):
        if close_all:
            return self._client.close_all()

    def _get(self, key):
        return self._client.get(key, self._db, decode_value=False)

    def _get_many(self, keys):
        return self._client.get_bulk(keys, self._db, decode_values=False)

    def _set(self, key, value, timeout):
        return self._client.set(key, value, self._db, timeout, self._no_reply,
                                encode_value=False)

    def _set_many(self, data, timeout):
        return self._client.set_bulk(data, self._db, timeout, self._no_reply,
                                     encode_values=False)

    def _delete(self, key):
        return self._client.remove(key, self._db, self._no_reply)

    def _delete_many(self, keys):
        return self._client.remove_bulk(keys, self._db, self._no_reply)

    def _flush(self):
        if not self.prefix:
            return self._client.clear()

        n = 0
        while True:
            keys = self._client.match_prefix(self.prefix, 1000, self._db)
            if not keys:
                break
            n += self._client.remove_bulk(keys, self._db, self._no_reply)
        return n


try:
    import kyotocabinet as kc
except ImportError:
    kc = None


class KCCache(Cache):
    manual_expire = True

    def __init__(self, filename, **params):
        self._filename = filename
        self._kc = None
        if kc is None:
            raise ImproperlyConfigured('Cannot use KCCache, kyotocabinet '
                                       'python bindings are not installed.')
        super(KCCache, self).__init__(**params)

    def open(self):
        if self._kc is not None:
            return False

        self._kc = kc.DB()
        mode = kc.DB.OWRITER | kc.DB.OCREATE | kc.DB.OTRYLOCK
        if not self._kc.open(self._filename, mode):
            raise UCacheException('kyotocabinet could not open cache '
                                  'database: "%s"' % self._filename)
        return True

    def close(self):
        if self._kc is None: return False
        self._kc.synchronize(True)
        if not self._kc.close():
            raise UCacheException('kyotocabinet error while closing cache '
                                  'database: "%s"' % self._filename)
        self._kc = None
        return True

    def _get(self, key):
        return self._kc.get(key)

    def _get_many(self, keys):
        return self._kc.get_bulk(keys)

    def _set(self, key, value, timeout):
        return self._kc.set(key, value)

    def _set_many(self, data, timeout):
        return self._kc.set_bulk(data)

    def _delete(self, key):
        return self._kc.remove(key)

    def _delete_many(self, keys):
        return self._kc.remove_bulk(keys)

    def _flush(self):
        if not self.prefix:
            self._kc.clear()
        else:
            keys = self._kc.match_prefix(self.prefix)
            if keys:
                self._kc.remove_bulk(keys)
        return self._kc.synchronize()

    def clean_expired(self, ndays=0):
        timestamp = time.time() - (ndays * 86400)

        class Visitor(kc.Visitor):
            n_deleted = 0
            def visit_full(self, key, value):
                ts, _ = decode_timestamp(value)
                if ts <= timestamp:
                    self.n_deleted += 1
                    return self.REMOVE
                else:
                    return self.NOP

            def visit_empty(self, key):
                return self.NOP

        visitor = Visitor()
        if not self._kc.iterate(visitor, True):
            raise UCacheException('kyotocabinet: error cleaning expired keys.')

        return visitor.n_deleted


try:
    from peewee import *
    try:
        from playhouse.sqlite_ext import CSqliteExtDatabase as SqliteDatabase
    except ImportError:
        from playhouse.sqlite_ext import SqliteExtDatabase as SqliteDatabase
except ImportError:
    SqliteDatabase = None


class SqliteCache(Cache):
    def __init__(self, filename, cache_size=32, thread_safe=True, **params):
        if SqliteDatabase is None:
            raise ImproperlyConfigured('Cannot use SqliteCache - peewee is '
                                       'not installed')
        self._filename = filename
        self._cache_size = cache_size  # In MiB.
        self._thread_safe = thread_safe
        self._db = SqliteDatabase(
            self._filename,
            thread_safe=self._thread_safe,
            pragmas={
                'cache_size': self._cache_size * -1000,
                'journal_mode': 'wal',  # Multiple readers + one writer.
                'synchronous': 0,
                'wal_synchronous': 0})

        class Cache(Model):
            key = TextField(primary_key=True)
            value = BlobField(null=True)
            expires = FloatField()

            class Meta:
                database = self._db
                indexes = (
                    (('key', 'expires'), False),
                )
                without_rowid = True

        self.cache = Cache
        super(SqliteCache, self).__init__(**params)

    def open(self):
        if not self._db.is_closed(): return False

        self._db.connect()
        self.cache.create_table()
        return True

    def close(self):
        if self._db.is_closed(): return False
        self._db.close()
        return True

    def _get(self, key):
        query = (self.cache
                 .select(self.cache.value)
                 .where((self.cache.key == key) &
                        (self.cache.expires >= time.time()))
                 .limit(1)
                 .tuples())
        try:
            return query.get()[0]
        except self.cache.DoesNotExist:
            pass

    def _get_many(self, keys):
        query = (self.cache
                 .select(self.cache.key, self.cache.value)
                 .where(self.cache.key.in_(keys) &
                        (self.cache.expires >= time.time()))
                 .tuples())
        return dict(query)

    def _set(self, key, value, timeout):
        expires = time.time() + timeout
        self.cache.replace(key=key, value=value, expires=expires).execute()

    def _set_many(self, data, timeout):
        expires = time.time() + timeout
        normalized = [(key, value, expires) for key, value in data.items()]
        fields = [self.cache.key, self.cache.value, self.cache.expires]
        return (self.cache
                .replace_many(normalized, fields=fields)
                .execute())

    def _delete(self, key):
        return self.cache.delete().where(self.cache.key == key).execute()

    def _delete_many(self, keys):
        return self.cache.delete().where(self.cache.key.in_(keys)).execute()

    def _flush(self):
        query = self.cache.delete()
        if self.prefix:
            prefix = decode(self.prefix)
            query = query.where(
                fn.SUBSTR(self.cache.key, 1, len(prefix)) == prefix)
        return query.execute()

    def clean_expired(self, ndays=0):
        timestamp = time.time() - (ndays * 86400)
        return (self.cache
                .delete()
                .where(self.cache.expires <= timestamp)
                .execute())


try:
    from redis import Redis
except ImportError:
    Redis = None

class RedisCache(Cache):
    def __init__(self, host='127.0.0.1', port=6379, db=0, connection=None,
                 **params):
        if Redis is None:
            raise ImproperlyConfigured('Cannot use RedisCache - redis python '
                                       'bindings are not installed.')
        self._host = host
        self._port = port
        self._db = db
        self._connection = connection
        self._client = None
        super(RedisCache, self).__init__(**params)

    def open(self):
        if self._client is not None: return False

        if self._connection is None:
            self._connection = Redis(host=self._host, port=self._port,
                                     db=self._db, **self.params)
        self._client = self._connection
        return True

    def close(self):
        if self._client is None:
            return False
        self._client = None
        return True

    def _set(self, key, value, timeout):
        return self._client.setex(key, timeout, value)

    def _get(self, key):
        return self._client.get(key)

    def _delete(self, key):
        return self._client.delete(key)

    def _get_many(self, keys):
        values = self._client.mget(keys)
        return dict(zip(keys, values))

    def _set_many(self, data, timeout):
        pipeline = self._client.pipeline()
        pipeline.mset(data)
        for key in data:
            pipeline.expire(key, timeout)
        return pipeline.execute()[0]

    def _delete_many(self, keys):
        return self._client.delete(*keys)

    def _flush(self):
        if not self.prefix:
            return self._client.flushdb()

        scanner = self._client.scan_iter(self.prefix + b'*')
        for key_list in chunked(scanner, 500):
            self._client.delete(*key_list)


try:
    import pylibmc as mc
except ImportError:
    mc = None

class MemcacheCache(Cache):
    def __init__(self, servers='127.0.0.1:11211', client=None, **params):
        if mc is None:
            raise ImproperlyConfigured('Cannot use MemcacheCache - pylibmc '
                                       'module is not installed.')
        self._servers = [servers] if isinstance(servers, str) else servers
        self._client = client
        super(MemcacheCache, self).__init__(**params)

    def open(self):
        if self._client is not None: return False
        self._client = mc.Client(self._servers, **self.params)
        return True

    def close(self):
        if self._client is None:
            return False
        self._client.disconnect_all()
        return True

    def _get(self, key):
        return self._client.get(key)

    def _get_many(self, keys):
        return self._client.get_multi(keys)

    def _set(self, key, value, timeout):
        return self._client.set(key, value, timeout)

    def _set_many(self, data, timeout):
        return self._client.set_multi(data, timeout)

    def _delete(self, key):
        return self._client.delete(key)

    def _delete_many(self, keys):
        return self._client.delete_multi(keys)

    def _flush(self):
        if self.prefix:
            raise NotImplementedError('Cannot flush memcached when a prefix '
                                      'is in use.')
        return self._client.flush_all()


try:
    import pymemcache
except ImportError:
    pymemcache = None

class PyMemcacheCache(MemcacheCache):
    def __init__(self, server=('127.0.0.1', 11211), client=None, **params):
        if pymemcache is None:
            raise ImproperlyConfigured('Cannot use PyMemcacheCache - '
                                       'pymemcache module is not installed.')
        if isinstance(server, str) and server.find(':') >= 0:
            host, port = server.rsplit(':', 1)
            server = (host, int(port))
        self._server = server
        self._client = client
        Cache.__init__(self, **params)

    def open(self):
        if self._client is not None: return False
        self._client = pymemcache.Client(self._server, **self.params)
        return True

    def close(self):
        if self._client is None: return False
        self._client.close()
        return True

    def __del__(self):
        if self._client is not None:
            self._client.close()


try:
    import dbm
except ImportError:
    try:
        from dbm import ndbm as dbm
    except ImportError:
        dbm = None


class DbmCache(MemoryCache):
    def __init__(self, filename, mode=None, *args, **kwargs):
        if dbm is None:
            raise ImproperlyConfigured('Cannot use DbmCache - dbm python '
                                       'module is not available.')
        self._filename = filename
        self._mode = mode or 0o644
        super(DbmCache, self).__init__(*args, **kwargs)

    def open(self, flag='c'):
        if self._data:
            return False
        self._data = dbm.open(self._filename, flag=flag, mode=self._mode)
        atexit.register(self._data.close)
        return True

    def close(self):
        if not self._data:
            return False
        atexit.unregister(self._data.close)
        self._data = None
        return True

    def _set_many(self, data, timeout):
        for key, value in data.items():
            self._set(key, value, timeout)

    def iter_keys(self):
        key = self._data.firstkey()
        while key is not None:
            if self._prefix_len == 0 or key.startswith(self.prefix):
                yield key
            key = self._data.nextkey(key)

    def _flush(self):
        with self._lock:
            if not self.prefix:
                self.close()
                self.open('n')
            else:
                self._delete_many(list(self.iter_keys()))

    def clean_expired(self, ndays=0):
        with self._lock:
            timestamp = time.time() - (ndays * 86400)
            accum = []
            for key in self.iter_keys():
                ts, _ = decode_timestamp(self._data[key])
                if ts <= timestamp:
                    accum.append(key)

            self._delete_many(accum)
        return len(accum)


try:
    from greendb import Client as GreenClient
except ImportError:
    GreenClient = None


class GreenDBCache(Cache):
    manual_expire = True

    def __init__(self, host='127.0.0.1', port=31337, db=0, pool=True,
                 max_age=None, **params):
        if GreenClient is None:
            raise ImproperlyConfigured('Cannot use GreenDBCache, greendb is '
                                       'not installed.')
        self._client = GreenClient(host, port, pool=pool, max_age=max_age)
        self._db = db
        super(GreenDBCache, self).__init__(**params)

    def open(self):
        if self._client.connect():
            self._client.use(self._db)
            return True
        return False

    def close(self):
        return self._client.close()

    def _get(self, key):
        return self._client.getraw(key)

    def _get_many(self, keys):
        return self._client.mgetraw(keys)

    def _set(self, key, value, timeout):
        return self._client.setraw(key, value)

    def _set_many(self, data, timeout):
        return self._client.msetraw(data)

    def _delete(self, key):
        return self._client.delete(key)

    def _delete_many(self, keys):
        return self._client.mdelete(keys)

    def _flush(self):
        if self.prefix:
            self._client.deleterange(self.prefix, self.prefix + b'\xff')
        else:
            self._client.flush()

    def clean_expired(self, ndays=0):
        timestamp = time.time() - (ndays * 86400)
        n_deleted = 0
        items = self._client.getrangeraw(self.prefix, self.prefix + b'\xff')
        for key, value in items:
            ts, _ = decode_timestamp(value)
            if ts <= timestamp:
                self._client.delete(key)
                n_deleted += 1
        return n_deleted
