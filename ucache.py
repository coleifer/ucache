from collections import ChainMap
from collections import Counter
from contextlib import contextmanager
import functools
import hashlib
import pickle
import time
import zlib
try:
    import msgpack
except ImportError:
    msgpack = None


__version__ = '0.1.1'
__all__ = [
    'KTCache',
    'RedisCache',
    'SqliteCache',
    'UC_NONE',
    'UC_MSGPACK',
    'UC_PICKLE',
]


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


decode = lambda s: s.decode('utf8') if isinstance(s, bytes) else s
encode = lambda s: s.encode('utf8') if isinstance(s, str) else s


UC_NONE = 0
UC_PICKLE = 1
UC_MSGPACK = 2


class Cache(object):
    def __init__(self, prefix=None, timeout=60, debug=False, compression=False,
                 compression_len=256, serializer=UC_PICKLE, connect=True,
                 **params):
        self.prefix = encode(prefix or '')
        self._prefix_len = len(self.prefix)
        self.timeout = timeout
        self.debug = debug
        self.compression = compression
        self.serializer = serializer
        self.params = params
        self._preload = ChainMap()  # Stack of preload data.

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

    @contextmanager
    def preload(self, keys):
        self._preload = self._preload.new_child(self.get_many(keys))
        yield
        self._preload = self._preload.parents

    def open(self):
        pass

    def close(self):
        pass

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
            return self._preload[key]

        data = self._get(self.prefix_key(key))
        if data is not None:
            return self.unpack(data)

    def _get(self, key):
        raise NotImplementedError

    def get_many(self, keys):
        if self.debug: return

        prefix_keys = [self.prefix_key(key) for key in keys]
        bulk_data = self._get_many(prefix_keys)
        accum = {}
        for key, data in bulk_data.items():
            if data is not None:
                accum[self.unprefix_key(key)] = self.unpack(data)
        return accum

    def _get_many(self, keys):
        raise NotImplementedError

    def set(self, key, value, timeout=None):
        if self.debug: return

        timeout = timeout if timeout is not None else self.timeout
        return self._set(self.prefix_key(key), self.pack(value), timeout)

    def _set(self, key, value, timeout):
        raise NotImplementedError

    def set_many(self, __data=None, timeout=None, **kwargs):
        if self.debug: return

        timeout = timeout if timeout is not None else self.timeout
        if __data is not None:
            kwargs.update(__data)

        accum = {}
        for key, value in kwargs.items():
            accum[self.prefix_key(key)] = self.pack(value)
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


try:
    from kt import KT_NONE
    from kt import KyotoTycoon
except ImportError:
    KyotoTycoon = None


class KTCache(Cache):
    def __init__(self, host='127.0.0.1', port=1978, db=0, connection_pool=True,
                 client_timeout=5, connection=None, no_reply=False, **params):
        if KyotoTycoon is None:
            raise Exception('Cannot use KTCache - kt is not installed')

        self._host = host
        self._port = port
        self._db = db
        self._pool = connection_pool
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
                           serializer=KT_NONE, connection_pool=self._pool)

    def open(self):
        return self._client.open()

    def close(self, close_all=False):
        if close_all and self._pool:
            return self._client.close_all()
        else:
            return self._client.close()

    def _get(self, key):
        return self._client.get_bytes(key, self._db)

    def _get_many(self, keys):
        return self._client.get_bulk(keys, self._db, decode_values=False)

    def _set(self, key, value, timeout):
        return self._client.set_bytes(key, value, self._db, timeout,
                                      self._no_reply)

    def _set_many(self, data, timeout):
        return self._client.set_bulk(data, self._db, timeout, self._no_reply,
                                     encode_values=False)

    def _delete(self, key):
        return self._client.remove(key, self._db, self._no_reply)

    def _delete_many(self, keys):
        return self._client.remove_bulk(keys, self._db, self._no_reply)

    def _flush(self):
        return self._client.clear()


try:
    from peewee import *
    try:
        from playhouse.sqlite_ext import CSqliteExtDatabase as SqliteDatabase
    except ImportError:
        from playhouse.sqlite_ext import SqliteExtDatabase as SqliteDatabase
except ImportError:
    SqliteDatabase = None


class SqliteCache(Cache):
    def __init__(self, filename, cache_size=32, **params):
        if SqliteDatabase is None:
            raise Exception('Cannot use SqliteCache - peewee is not installed')
        self._filename = filename
        self._cache_size = cache_size  # In MiB.
        self._db = SqliteDatabase(self._filename, pragmas={
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
        return self.cache.delete().execute()

    def clean_expired(self, n_days=0):
        timestamp = time.time() - (n_days * 86400)
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
            raise Exception('Cannot use RedisCache - redis is not installed')
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
        return self._client.flushdb()
