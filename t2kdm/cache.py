"""A cache for grid tool output to make CLI experience more snappy."""

from time import time
from cPickle import dumps

class CacheEntry(object):
    """An entry in the cache."""

    def __init__(self, value, creation_time=None, cache_time=60):
        self.value = value
        if creation_time is None:
            self.creation_time = time()
        self.cache_time = cache_time

    def is_valid(self):
        return (self.creation_time + self.cache_time) > time()

class Cache(object):
    """A simple cache for function calls."""

    def __init__(self, cache_time=60):
        """`cache_time` determines how long an entry will be cached."""
        self.cache_time = cache_time
        self.cache = {}

    def clean(self):
        """Remove old entries from the cache."""
        for key in self.cache.keys():
            if not self.cache[key].is_valid():
                del self.cache[key]

    def flush(self):
        """Remove all entries from the cache."""
        for key in self.cache.keys():
            del self.cache[key]

    def hash(self, function, *args, **kwargs):
        """Turn function parameters into a hash."""
        # Protect against non-pickleable `self`s
        args = list(args)
        if len(args) > 0:
            args[0] = repr(args[0])
        return hash(dumps( (function.func_name, args, kwargs) ))

    def get_entry(self, function, *args, **kwargs):
        """Get a valid entry from the cache or `None`."""
        key = self.hash(function, *args, **kwargs)
        if key in self.cache:
            entry = self.cache[key]
            if entry.is_valid():
                return entry
            else:
                return None
        else:
            return None

    def add_entry(self, value, function, *args, **kwargs):
        """Add an entry to the cache."""
        key = self.hash(function, *args, **kwargs)
        self.cache[key] = CacheEntry(value, cache_time=self.cache_time)

    def cached(self, function):
        """Decorator to turn a regular function into a cached one."""

        def cached_function(*args, **kwargs):
            cached = kwargs.pop('cached', False)
            if cached:
                entry = self.get_entry(function, *args, **kwargs)
                if entry is not None:
                    return entry.value
                else:
                    value = function(*args, **kwargs)
                    self.add_entry(value, function, *args, **kwargs)
                    return value
            else:
                return function(*args, **kwargs)

        return cached_function
