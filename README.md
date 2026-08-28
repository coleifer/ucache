![](http://media.charlesleifer.com/blog/photos/ucache-logo-0.png)

ucache is a lightweight and efficient caching library for python.

* [Kyoto Tycoon](https://fallabs.com/kyototycoon/) via [ukt](https://github.com/coleifer/ukt)
* [Redis](https://redis.io) via [redis-py](https://github.com/andymccurdy/redis-py)
* [Memcached](https://memcached.org) via [pymemcache](https://github.com/pinterest/pymemcache)
* [Sqlite](https://www.sqlite.org/) via [peewee](https://github.com/coleifer/peewee)
* Simple in-memory cache using Python dictionary.

Features:

* Pickle serialization by default, msgpack also supported
* Optional (transparent) compression using `zlib`
* Efficient bulk-operations.
* Preload context manager for efficient pre-loading of cached data
