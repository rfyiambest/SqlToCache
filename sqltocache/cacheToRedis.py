"""
A simple redis-cache interface for storing python objects.
"""
from functools import wraps
import pickle
import json
import msgpack
import hashlib
import redis
import logging

class RedisConnect(object):
    def __init__(self, host=None, port=None, db=None, password=None):
        self.host = host if host else 'localhost'
        self.port = port if port else 6379
        self.db = db if db else 0
        self.password = password

    def connect(self):
        try:
            redis.StrictRedis(host=self.host, port=self.port, password=self.password).ping()
        except redis.ConnectionError as e:
            raise RedisNoConnException("Failed to create connection to redis",
                                       (self.host,
                                        self.port)
            )
        return redis.StrictRedis(host=self.host,
                                 port=self.port,
                                 db=self.db,
                                 password=self.password)


class CacheMissException(Exception):
    pass


class ExpiredKeyException(Exception):
    pass


class RedisNoConnException(Exception):
    pass


class DoNotCache(Exception):
    _result = None

    def __init__(self, result):
        super(DoNotCache, self).__init__()
        self._result = result

    @property
    def result(self):
        return self._result


class SimpleCache(object):
    def __init__(self,
                 limit=10000,
                 expire=60 * 60 * 24,
                 hashkeys=False,
                 host=None,
                 port=None,
                 db=None,
                 password=None,
                 namespace="SimpleCache"):

        self.limit = limit  # No of json encoded strings to cache
        self.expire = expire  # Time to keys to expire in seconds
        self.prefix = namespace
        self.host = host
        self.port = port
        self.db = db

        try:
            self.connection = RedisConnect(host=self.host,
                                           port=self.port,
                                           db=self.db,
                                           password=password).connect()
        except RedisNoConnException, e:
            self.connection = None
            pass

        # Should we hash keys? There is a very small risk of collision invloved.
        self.hashkeys = hashkeys

    def make_key(self, key):
        return "SimpleCache-{0}:{1}".format(self.prefix, key)

    def namespace_key(self, namespace):
        return self.make_key(namespace + ':*')

    def get_set_name(self):
        return "SimpleCache-{0}-keys".format(self.prefix)

    def store(self, key, value, expire=None):
        key = to_unicode(key)
        value = to_unicode(value)
        set_name = self.get_set_name()

        while self.connection.scard(set_name) >= self.limit:
            del_key = self.connection.spop(set_name)
            self.connection.delete(self.make_key(del_key))

        pipe = self.connection.pipeline()
        if expire is None:
            expire = self.expire
        pipe.setex(self.make_key(key), expire, value)
        pipe.sadd(set_name, key)
        pipe.execute()


    def expire_all_in_set(self):
        all_members = self.keys()

        with self.connection.pipeline() as pipe:
            pipe.delete(*all_members)
            pipe.execute()

        return len(self), len(all_members)

    def expire_namespace(self, namespace):
        namespace = self.namespace_key(namespace)
        all_members = list(self.connection.keys(namespace))
        with self.connection.pipeline() as pipe:
            pipe.delete(*all_members)
            pipe.execute()

        return len(self), len(all_members)

    def isexpired(self, key):
        ttl = self.connection.pttl("SimpleCache-{0}".format(key))
        if ttl == -1:
            return True
        if not ttl is None:
            return ttl
        else:
            return self.connection.pttl("{0}:{1}".format(self.prefix, key))

    def store_json(self, key, value):
        self.store(key, json.dumps(value))

    def store_pickle(self, key, value):
        self.store(key, pickle.dumps(value))

    def get(self, key):
        key = to_unicode(key)
        if key:  # No need to validate membership, which is an O(1) operation, but seems we can do without.
            value = self.connection.get(self.make_key(key))
            if value is None:  # expired key
                if not key in self:  # If key does not exist at all, it is a straight miss.
                    raise CacheMissException

                self.connection.srem(self.get_set_name(), key)
                raise ExpiredKeyException
            else:
                return value

    def mget(self, keys):
        if keys:
            cache_keys = [self.make_key(to_unicode(key)) for key in keys]
            values = self.connection.mget(cache_keys)

            if None in values:
                pipe = self.connection.pipeline()
                for cache_key, value in zip(cache_keys, values):
                    if value is None:  # non-existant or expired key
                        pipe.srem(self.get_set_name(), cache_key)
                pipe.execute()

            return {k: v for (k, v) in zip(keys, values) if v is not None}

    def get_json(self, key):
        return json.loads(self.get(key))

    def get_pickle(self, key):
        return pickle.loads(self.get(key))

    def mget_json(self, keys):
        d = self.mget(keys)
        if d:
            for key in d.keys():
                d[key] = json.loads(d[key]) if d[key] else None
            return d

    def invalidate(self, key):
        key = to_unicode(key)
        pipe = self.connection.pipeline()
        pipe.srem(self.get_set_name(), key)
        pipe.delete(self.make_key(key))
        pipe.execute()

    def __contains__(self, key):
        return self.connection.sismember(self.get_set_name(), key)

    def __iter__(self):
        if not self.connection:
            return iter([])
        return iter(
            ["{0}:{1}".format(self.prefix, x)
                for x in self.connection.smembers(self.get_set_name())
            ])

    def __len__(self):
        return self.connection.scard(self.get_set_name())

    def keys(self):
        return self.connection.smembers(self.get_set_name())


    def flush(self):
        keys = list(self.keys())
        keys.append(self.get_set_name())
        with self.connection.pipeline() as pipe:
            pipe.delete(*keys)
            pipe.execute()

    def flush_namespace(self, namespace):
        namespace = self.namespace_key(namespace)
        setname = self.get_set_name()
        keys = list(self.connection.keys(namespace))
        with self.connection.pipeline() as pipe:
            pipe.delete(*keys)
            pipe.srem(setname, *keys)
            pipe.execute()

    def get_hash(self, args):
        if self.hashkeys:
            key = hashlib.md5(args).hexdigest()
        else:
            key = pickle.dumps(args)
        return key


def cache_it(limit=10000, expire=60 * 60 * 24, cache=None,
             use_json=False, namespace=None):
    cache_ = cache
    def decorator(function):
        cache = cache_
        if cache is None:
            cache = SimpleCache(limit, expire, hashkeys=True, namespace=function.__module__)

        @wraps(function)
        def func(*args, **kwargs):
            ## Handle cases where caching is down or otherwise not available.
            if cache.connection is None:
                result = function(*args, **kwargs)
                return result

            serializer = json if use_json else pickle
            fetcher = cache.get_json if use_json else cache.get_pickle
            storer = cache.store_json if use_json else cache.store_pickle

            key = cache.get_hash(serializer.dumps([args, kwargs]))
            cache_key = '{func_name}:{key}'.format(func_name=function.__name__,
                                                   key=key)

            if namespace:
                cache_key = '{namespace}:{key}'.format(namespace=namespace,
                                                       key=cache_key)

            try:
                return fetcher(cache_key)
            except (ExpiredKeyException, CacheMissException) as e:
                ## Add some sort of cache miss handing here.
                pass
            except:
                logging.exception("Unknown redis-simple-cache error. Please check your Redis free space.")

            try:
                result = function(*args, **kwargs)
            except DoNotCache as e:
                result = e.result
            else:
                try:
                    storer(cache_key, result)
                except redis.ConnectionError as e:
                    logging.exception(e)

            return result
        return func
    return decorator


def cache_it_msgpack(limit=10000, expire=60 * 60 * 24, cache=None, namespace=None):
    return cache_it(limit=limit, expire=expire, use_json=True,
                    cache=cache, namespace=None)


def to_unicode(obj, encoding='utf-8'):
    if isinstance(obj, basestring):
        if not isinstance(obj, unicode):
            obj = unicode(obj, encoding)
    return obj
