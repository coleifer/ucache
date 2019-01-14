## ucache

Lightweight and efficient caching library for Python.

* [Kyoto Tycoon](https://fallabs.com/kyototycoon/) via [kt](https://github.com/coleifer/kt)
* [Redis](https://redis.io) via [redis-py](https://github.com/andymccurdy/redis-py)
* [Sqlite](https://www.sqlite.org/) via [peewee](https://github.com/coleifer/peewee)

Features:

* Pickle serialization by default, msgpack also supported
* Optional (transparent) compression using `zlib`
* Efficient bulk-operations.
* Preload context manager for efficient pre-loading of cached data
