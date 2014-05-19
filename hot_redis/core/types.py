# -*- coding: utf-8 -*-
import collections
import operator
import os
import time
import uuid
from Queue import Empty as QueueEmpty, Full as QueueFull
from itertools import chain, repeat

from redis import ResponseError
from redis.client import Redis, zset_score_pairs


class Ranking(object):
    def __init__(self, ranking_script, keys_, *script_args):
        if len(keys_) < 2:
            raise ValueError("You have to have at least two lists to compare")

        self.keys = keys_
        self.ranker = ranking_script
        self.script_args = script_args

    def __getitem__(self, i):
        if not isinstance(i, slice):
            args = [i, i]
            args.extend(self.script_args)
            response = self.ranker(keys=self.keys, args=args)
            return zset_score_pairs(response, withscores=True)

        start = i.start if i.start is not None else 0
        stop = i.stop if i.stop is not None else 0

        args = [start, stop - 1]
        args.extend(self.script_args)
        response = self.ranker(keys=self.keys, args=args)
        return zset_score_pairs(response, withscores=True)

    def __iter__(self):
        return iter(self[:])


class HotClient(object):
    """
    A Redis client wrapper that loads Lua functions and creates
    client methods for calling them.
    """
    _ATOMS_FILE_NAME = "core_atoms.lua"
    _BIT_FILE_NAME = "bit.lua"
    _MULTI_FILE_NAME = "multi.lua"
    _BLIST_FILE_NAME = "blist_atoms.lua"

    def __init__(self, client=None, *args, **kwargs):
        self._client = client
        if not self._client:
            self._client = Redis(*args, **kwargs)

        self._bind_atoms()
        self._bind_multi()
        self._bind_blist()

    def _bind_atoms(self):
        with open(self._get_lua_path(self._BIT_FILE_NAME)) as f:
            luabit = f.read()

        requires_luabit = (
            "number_and",
            "number_or",
            "number_xor",
            "number_lshift",
            "number_rshift"
        )

        for name, snippet in self._split_lua_file_into_funcs(
                self._ATOMS_FILE_NAME):
            if name in requires_luabit:
                snippet = luabit + snippet
            self._bind_lua_method(name, snippet)

    def _bind_multi(self):
        for name, snippet in self._split_lua_file_into_funcs(
            self._MULTI_FILE_NAME):
            self._bind_private_lua_script(name, snippet)

    def _bind_blist(self):
        for name, snippet in self._split_lua_file_into_funcs(
            self._BLIST_FILE_NAME):
            self._bind_lua_method(name, snippet)

    @staticmethod
    def _get_lua_path(name):
        """
        Joins the given name with the relative path of the module.
        """
        parts = (os.path.dirname(os.path.abspath(__file__)), "lua", name)
        return os.path.join(*parts)

    def _split_lua_file_into_funcs(self, file_name):
        """
        Returns the name / code snippet pair for each Lua function
        in the file under file_name.
        """
        with open(self._get_lua_path(file_name)) as f:
            for func in f.read().strip().split("function "):
                if func:
                    bits = func.split("\n", 1)
                    name = bits[0].split("(")[0].strip()
                    snippet = bits[1].rsplit("end", 1)[0].strip()
                    yield name, snippet

    def _bind_lua_method(self, name, code):
        """
        Registers the code snippet as a Lua script, and binds the
        script to the client as a method that can be called with
        the same signature as regular client methods, eg with a
        single key arg.
        """
        script = self._client.register_script(code)
        method = lambda key, *a, **k: script(keys=[key], args=a, **k)
        setattr(self, name, method)

    def _bind_private_lua_script(self, name, code):
        """
        Registers the code snippet as a Lua script, and binds the
        script to the client as a private method (eg. some_lua_func becomes
        a _some_lua_func method of HotClient) that can be latter wrapped in
        public methods with better argument and error handling.
        """
        script = self._client.register_script(code)
        setattr(self, '_' + name, script)

    def rank_lists_by_length(self, *keys):
        """
        Creates a temporary ZSET with LIST keys as entries and their
        *LLEN* as scores. Uses ZREVRANGE .. WITHSCORES, to return keys and
        lengths sorted from longest to shortests.
        :param keys: keys of the lists you want rank
        :return: :rtype: Ranking :raise ValueError:
        :raise ValueError: when not enough keys are provided
        """
        return Ranking(self._rank_lists_by_length, keys)

    def rank_sets_by_cardinality(self, *keys):
        """
        Creates a temporary ZSET with SET keys as entries and their
        *CARD* as scores. Uses ZREVRANGE .. WITHSCORES, to return keys and
        cardinalities sorted from largest to smallest.
        :param keys: keys of the sets you want to rank
        :return: :rtype: Ranking
        :raise ValueError: when not enough keys are provided
        """
        return Ranking(self._rank_sets_by_cardinality, keys)

    def rank_zsets_by_cardinality(self, *keys):
        """
        Creates a temporary ZSET with SET keys as entries and their
        *ZCARD* as scores. Uses ZREVRANGE .. WITHSCORES, to return keys and
        cardinalities sorted from largest to smallest.
        :param keys: keys of the sets you want to rank
        :return: :rtype: Ranking
        :raise ValueError: when not enough keys are provided
        """
        return Ranking(self._rank_zsets_by_cardinality, keys)

    def rank_by_sum_of_decaying_score(
            self, from_, halflife, cache_timeout, *keys):
        """
        Creates a temporary ZSET with SET keys as entries and sums of their
        decayed (standard halflife decay function) scores as scores. Uses
        ZREVRANGE .. WITHSCORES, to return
        keys and these sums sorted from largest to smallest. Useful for
        finiding recently most active zsets assuming that the score is a
        unix timestamp.
        :param from_: current time in unix timestamp, the difference from_ -
        SCORE is used as age of the key in ZSET
        :param halflife: the halflife of the decay function
        :param cache_timeout: if greater than 0 the resulting sum of scores
        of each ZSET in key's will be cached for that many seconds. Then
        when this function will be called again with the same halflife the
        cached value will be returned (from_ will have no effect)
        :param keys: keys of the zsets you want to rank
        :return: :rtype: Ranking
        :raise ValueError: when not enough keys are provided
        """
        return Ranking(self._rank_by_sum_of_decaying_score, keys, from_,
                       halflife, cache_timeout)

    def __getattr__(self, name):
        if name in self.__dict__:
            return super(HotClient, self).__getattribute__(name)
        return self._client.__getattribute__(name)


_client = None
_config = {}


def default_client():
    global _client
    if _client is None:
        _client = HotClient(**_config)
    return _client


def configure(config):
    global _config
    _config = config


####################################################################
#                                                                  #
#  The following functions are all used for creating methods that  #
#  get assigned to all the magic operator names for each type,     #
#  and make much more sense further down where they're applied.    #
#                                                                  #
####################################################################

def value_left(self, other):
    """
    Returns the value of the other type instance to use in an
    operator method, namely when the method's instance is on the
    left side of the expression.
    """
    return other.value if isinstance(other, self.__class__) else other


def value_right(self, other):
    """
    Returns the value of the type instance calling an to use in an
    operator method, namely when the method's instance is on the
    left side of the expression.
    """
    return self if isinstance(other, self.__class__) else self.value


def op_left(op):
    """
    Returns a type instance method for the given operator, applied
    when the instance appears on the left side of the expression.
    """

    def method(self, other):
        return op(self.value, value_left(self, other))

    return method


def op_right(op):
    """
    Returns a type instance method for the given operator, applied
    when the instance appears on the right side of the expression.
    """

    def method(self, other):
        return op(value_left(self, other), value_right(self, other))

    return method


def inplace(method_name):
    """
    Returns a type instance method that will call the given method
    name, used for inplace operators such as __iadd__ and __imul__.
    """

    def method(self, other):
        getattr(self, method_name)(value_left(self, other))
        return self

    return method


#####################################################################
#                                                                   #
#  Base class / groupings of logical operators that types inherit.  #
#                                                                   #
#####################################################################

class Base(object):
    """
    Base type that all others inherit. Contains the basic comparison
    operators as well as the dispatch for proxying to methods on the
    Redis client.
    """

    def __init__(self, initial=None, redis_key=None, client=None):
        if not isinstance(client, HotClient):
            client = HotClient(client) or default_client()
        else:
            client = client or default_client()
        self.client = client

        self.key = redis_key or str(uuid.uuid4())
        if initial:
            self.value = initial

    __eq__ = op_left(operator.eq)
    __lt__ = op_left(operator.lt)
    __le__ = op_left(operator.le)
    __gt__ = op_left(operator.gt)
    __ge__ = op_left(operator.ge)

    def __repr__(self):
        bits = (self.__class__.__name__, repr(self.value), self.key)
        return "%s(%s, '%s')" % bits

    def __getattr__(self, name):
        return self._dispatch(name)

    def _dispatch(self, name):
        try:
            func = getattr(self.client, name)
        except AttributeError:
            raise
        return lambda *a, **k: func(self.key, *a, **k)


class Bitwise(Base):
    """
    Base class for bitwise types and relevant operators.
    """
    __and__ = op_left(operator.and_)
    __or__ = op_left(operator.or_)
    __xor__ = op_left(operator.xor)
    __lshift__ = op_left(operator.lshift)
    __rshift__ = op_left(operator.rshift)
    __rand__ = op_right(operator.and_)
    __ror__ = op_right(operator.or_)
    __rxor__ = op_right(operator.xor)
    __rlshift__ = op_right(operator.lshift)
    __rrshift__ = op_right(operator.rshift)


class Sequential(Base):
    """
    Base class for sequence types and relevant operators.
    """
    __add__ = op_left(operator.add)
    __mul__ = op_left(operator.mul)
    __radd__ = op_right(operator.add)
    __rmul__ = op_right(operator.mul)


class Numeric(Base):
    """
    Base class for numeric types and relevant operators.
    """
    __add__ = op_left(operator.add)
    __sub__ = op_left(operator.sub)
    __mul__ = op_left(operator.mul)
    __div__ = op_left(operator.div)
    __floordiv__ = op_left(operator.floordiv)
    __truediv__ = op_left(operator.truediv)
    __mod__ = op_left(operator.mod)
    __divmod__ = op_left(divmod)
    __pow__ = op_left(operator.pow)
    __radd__ = op_right(operator.add)
    __rsub__ = op_right(operator.sub)
    __rmul__ = op_right(operator.mul)
    __rdiv__ = op_right(operator.div)
    __rtruediv__ = op_right(operator.truediv)
    __rfloordiv__ = op_right(operator.floordiv)
    __rmod__ = op_right(operator.mod)
    __rdivmod__ = op_right(divmod)
    __rpow__ = op_right(operator.pow)
    __iadd__ = inplace("incr")
    __isub__ = inplace("decr")
    __imul__ = inplace("number_multiply")
    __idiv__ = inplace("number_divide")
    __ifloordiv__ = inplace("number_floordiv")
    __imod__ = inplace("number_mod")
    __ipow__ = inplace("number_pow")


####################################################
#                                                  #
#  Python types that map directly to Redis types.  #
#                                                  #
####################################################

class List(Sequential):
    """
    Redis list <-> Python list
    """

    @property
    def value(self):
        return self[:]

    @value.setter
    def value(self, value):
        self.extend(value)

    __iadd__ = inplace("extend")
    __imul__ = inplace("list_multiply")

    def __len__(self):
        return self.llen()

    def __setitem__(self, i, item):
        try:
            self.lset(i, item)
        except ResponseError:
            raise IndexError

    def __getitem__(self, i):
        if isinstance(i, slice):
            start = i.start if i.start is not None else 0
            stop = i.stop if i.stop is not None else 0
            return self.lrange(start, stop - 1)
        item = self.lindex(i)
        if item is None:
            raise IndexError
        return item

    def __delitem__(self, i):
        self.pop(i)

    def __iter__(self):
        return iter(self.value)

    def append(self, item):
        self.extend([item])

    def extend(self, other):
        self.rpush(*other)

    def insert(self, i, item):
        self.list_insert(i, item)

    def pop(self, i=-1):
        if i == -1:
            return self.rpop()
        elif i == 0:
            return self.lpop()
        else:
            return self.list_pop(i)

    def reverse(self):
        self.list_reverse()

    def index(self, item):
        return self.value.index(item)

    def count(self, item):
        return self.value.count(item)

    def sort(self, reverse=False):
        self._dispatch("sort")(desc=reverse, store=self.key, alpha=True)


class Set(Bitwise):
    """
    Redis set <-> Python set
    """

    @property
    def value(self):
        return self.smembers()

    @value.setter
    def value(self, item):
        self.update(item)

    def _all_redis(self, sets):
        return all([isinstance(s, self.__class__) for s in sets])

    def _to_keys(self, sets):
        return [s.key for s in sets]

    __iand__ = inplace("intersection_update")
    __ior__ = inplace("update")
    __ixor__ = inplace("symmetric_difference_update")
    __isub__ = inplace("difference_update")
    __rsub__ = op_right(operator.sub)

    def __and__(self, value):
        return self.intersection(value)

    def __or__(self, value):
        return self.union(value)

    def __xor__(self, value):
        return self.symmetric_difference(value)

    def __sub__(self, value):
        return self.difference(value)

    def __len__(self):
        return self.scard()

    def __contains__(self, item):
        return self.sismember(item)

    def __iter__(self):
        return iter(self.value)

    def add(self, item):
        self.update([item])

    def update(self, *sets):
        self.sadd(*reduce(operator.or_, sets))

    def pop(self):
        return self.spop()

    def clear(self):
        self.delete()

    def remove(self, item):
        if self.srem(item) == 0:
            raise KeyError(item)

    def discard(self, item):
        try:
            self.remove(item)
        except KeyError:
            pass

    def intersection(self, *sets):
        if self._all_redis(sets):
            return self.sinter(*self._to_keys(sets))
        else:
            return reduce(operator.and_, (self.value,) + sets)

    def intersection_update(self, *sets):
        if self._all_redis(sets):
            self.sinterstore(self.key, *self._to_keys(sets))
        else:
            sets = list(reduce(operator.and_, sets))
            self.set_intersection_update(*sets)
        return self

    def union(self, *sets):
        if self._all_redis(sets):
            return self.sunion(*self._to_keys(sets))
        else:
            return reduce(operator.or_, (self.value,) + sets)

    def difference(self, *sets):
        if self._all_redis(sets):
            return self.sdiff(*self._to_keys(sets))
        else:
            return reduce(operator.sub, (self.value,) + sets)

    def difference_update(self, *sets):
        if self._all_redis(sets):
            self.sdiffstore(self.key, *self._to_keys(sets))
        else:
            key = str(uuid.uuid4())
            flattened = [key]
            for s in sets:
                flattened.extend(s)
                flattened.append(key)
            self.set_difference_update(*flattened)
        return self

    def symmetric_difference(self, other):
        if isinstance(other, self.__class__):
            return set(self.set_symmetric_difference("return", other.key))
        else:
            return self.value ^ other

    def symmetric_difference_update(self, other):
        if isinstance(other, self.__class__):
            self.set_symmetric_difference("update", other.key)
        else:
            self.set_symmetric_difference("create", *other)
        return self

    def isdisjoint(self, other):
        return not self.intersection(other)

    def issubset(self, other):
        return self <= other

    def issuperset(self, other):
        return self >= other


class Dict(Base):
    """
    Redis hash <-> Python dict
    """

    @property
    def value(self):
        return self.hgetall()

    @value.setter
    def value(self, value):
        if not isinstance(value, dict):
            try:
                value = dict(value)
            except TypeError:
                value = None
        if value:
            self.update(value)

    def __len__(self):
        return self.hlen()

    def __contains__(self, key):
        return self.hexists(key)

    def __iter__(self):
        return self.iterkeys()

    def __setitem__(self, key, value):
        self.hset(key, value)

    def __getitem__(self, key):
        value = self.get(key)
        if value is None:
            raise KeyError(key)
        return value

    def __delitem__(self, key):
        if self.hdel(key) == 0:
            raise KeyError(key)

    def update(self, value):
        self.hmset(value)

    def keys(self):
        return self.hkeys()

    def values(self):
        return self.hvals()

    def items(self):
        return self.value.items()

    def iterkeys(self):
        return iter(self.keys())

    def itervalues(self):
        return iter(self.values())

    def iteritems(self):
        return iter(self.items())

    def setdefault(self, key, value=None):
        if self.hsetnx(key, value) == 1:
            return value
        else:
            return self.get(key)

    def get(self, key, default=None):
        value = self.hget(key)
        return value if value is not None else default

    def has_key(self, key):
        return key in self

    def copy(self):
        return self.__class__(self.value)

    def clear(self):
        self.delete()

    @classmethod
    def fromkeys(cls, *args):
        if len(args) == 1:
            args += ("",)
        return cls({}.fromkeys(*args))


class String(Sequential):
    """
    Redis string <-> Python string (although mutable).
    """

    @property
    def value(self):
        return self.get() or ""

    @value.setter
    def value(self, value):
        if value:
            self.set(value)

    __iadd__ = inplace("append")
    __imul__ = inplace("string_multiply")

    def __len__(self):
        return self.strlen()

    def __setitem__(self, i, s):
        if isinstance(i, slice):
            start = i.start if i.start is not None else 0
            stop = i.stop
        else:
            start = i
            stop = None
        if stop is not None and stop < start + len(s):
            self.string_setitem(start, stop, s)
        else:
            self.setrange(start, s)

    def __getitem__(self, i):
        if not isinstance(i, slice):
            i = slice(i, i + 1)
        start = i.start if i.start is not None else 0
        stop = i.stop if i.stop is not None else 0
        s = self.getrange(start, stop - 1)
        if not s:
            raise IndexError
        return s

    def __iter__(self):
        return iter(self.value)


class ImmutableString(String):
    """
    Redis string <-> Python string (actually immutable).
    """

    def __iadd__(self, other):
        self.key = self.__class__(self + other).key
        return self

    def __imul__(self, i):
        self.key = self.__class__(self * i).key
        return self

    def __setitem__(self, i):
        raise TypeError


class Int(Numeric, Bitwise):
    """
    Redis integer <-> Python integer.
    """

    @property
    def value(self):
        return int(float(self.get() or 0))

    @value.setter
    def value(self, value):
        if value:
            self.set(value)

    __iand__ = inplace("number_and")
    __ior__ = inplace("number_or")
    __ixor__ = inplace("number_xor")
    __ilshift__ = inplace("number_lshift")
    __irshift__ = inplace("number_rshift")


class Float(Numeric):
    """
    Redis float <-> Python float.
    """

    @property
    def value(self):
        return float(self.get() or 0)

    @value.setter
    def value(self, value):
        if value:
            self.set(value)

    __iadd__ = inplace("incrbyfloat")

    def __isub__(self, f):
        self.incrbyfloat(f * -1)
        return self


###################################################################
#                                                                 #
#  Following are the types found in the Python standard library,  #
#  that can be represented by extending / composing the above     #
#  types. First up: Queue module.                                 #
#                                                                 #
###################################################################

class Queue(List):
    """
    Redis list <-> Python list <-> Python's Queue.Queue.
    """

    maxsize = 0

    def __init__(self, maxsize=None, **kwargs):
        if maxsize is not None:
            self.maxsize = maxsize
        super(Queue, self).__init__(**kwargs)

    @property
    def queue(self):
        return self

    def qsize(self):
        return len(self)

    def empty(self):
        return self.qsize() == 0

    def full(self):
        return self.maxsize > 0 and self.qsize() >= self.maxsize

    def put(self, item, block=True, timeout=None):
        if self.maxsize == 0:
            self.append(item)
        else:
            if not block:
                timeout = 0
            start = time.time()
            while True:
                if self.queue_put(item, self.maxsize):
                    break
                if timeout is not None and time.time() - start >= timeout:
                    raise QueueFull
                time.sleep(.1)

    def put_nowait(self, item):
        return self.put(item, block=False)

    def get(self, block=True, timeout=None):
        if block:
            item = self.blpop(timeout=timeout)
            if item is not None:
                item = item[1]
        else:
            item = self.pop()
        if item is None:
            raise QueueEmpty
        return item

    def get_nowait(self):
        return self.get(block=False)

    def join(self):
        while not self.empty():
            time.sleep(.1)


class LifoQueue(Queue):
    """
    Redis list <-> Python list <-> Python's Queue.LifoQueue.
    """

    def append(self, item):
        self.lpush(item)


class SetQueue(Queue):
    """
    Redis list + Redis set <-> Queue with only unique items.
    """

    def __init__(self, *args, **kwargs):
        super(SetQueue, self).__init__(*args, **kwargs)
        self.set = Set(redis_key="%s-set" % self.key)

    def get(self, *args, **kwargs):
        item = super(SetQueue, self).get(*args, **kwargs)
        self.set.remove(item)
        return item

    def put(self, item, *args, **kwargs):
        if self.set.sadd(item) > 0:
            super(SetQueue, self).put(item, *args, **kwargs)

    def delete(self):
        self._dispatch("delete")
        self.set.delete()


class LifoSetQueue(LifoQueue, SetQueue):
    """
    Redis list + Redis set <-> LifoQueue with only unique items.
    """
    pass


####################################################################
#                                                                  #
#  Next up, some lock structures from the threading module. These  #
#  are all backed by the above Queue class, since it provides the  #
#  blocking / non-blocking mechanics desired.                      #
#                                                                  #
####################################################################

class BoundedSemaphore(Queue):
    """
    Redis list <-> Python list <-> Queue <-> threading.BoundedSemaphore.

    BoundedSemaphore's ``value`` arg maps to Queue's ``maxsize``.
    BoundedSemaphore's acquire/release methods maps to Queue's put/get
    methods repectively, providing blocking/timeout mechanics.
    """

    maxsize = 1

    def __init__(self, value=None, **kwargs):
        super(BoundedSemaphore, self).__init__(value, **kwargs)

    def acquire(self, block=True, timeout=None):
        try:
            self.put(1, block, timeout)
        except QueueFull:
            return False
        self.acquired = True
        return True

    def release(self):
        if not getattr(self, "acquired", False):
            raise RuntimeError("Cannot release unacquired lock")
        self.acquired = False
        self.get()

    def __enter__(self):
        self.acquire()

    def __exit__(self, t, v, tb):
        self.release()


class Semaphore(BoundedSemaphore):
    """
    Redis list <-> Python list <-> Queue <-> threading.Semaphore.

    Same implementation as BoundedSemaphore, but without a queue size.
    """

    def release(self):
        try:
            super(Semaphore, self).release()
        except RuntimeError:
            pass


class Lock(BoundedSemaphore):
    """
    Redis list <-> Python list <-> Queue <-> threading.Lock.

    Same implementation as BoundedSemaphore, but with a fixed
    queue size of 1.
    """

    def __init__(self, **kwargs):
        kwargs["value"] = None
        super(Lock, self).__init__(**kwargs)


class RLock(Lock):
    """
    Redis list <-> Python list <-> Queue <-> threading.RLock.

    Same implementation as BoundedSemaphore, but with a fixed
    queue size of 1, and as per re-entrant locks, can be acquired
    multiple times.
    """

    def __init__(self, *args, **kwargs):
        self.acquires = 0
        super(RLock, self).__init__(*args, **kwargs)

    def acquire(self, *args, **kwargs):
        result = True
        if self.acquires == 0:
            result = super(RLock, self).acquire(*args, **kwargs)
        if result:
            self.acquires += 1
        return result

    def release(self):
        if self.acquires > 1:
            self.acquires -= 1
            if self.acquires > 0:
                return
        super(RLock, self).release()


#####################################################################
#                                                                   #
#  Some members from Python's collections standard library module.  #
#                                                                   #
#####################################################################


class DefaultDict(Dict):
    """
    Redis hash <-> Python dict <-> Python's collections.DefaultDict.
    """

    def __init__(self, default_factory, *args, **kwargs):
        self.default_factory = default_factory
        super(DefaultDict, self).__init__(*args, **kwargs)

    def __getitem__(self, key):
        return self.setdefault(key, self.default_factory())


class MultiSet(collections.MutableMapping, Base):
    """
    Redis sorted set <-> Python's collections.Counter.
    """

    @classmethod
    def fromkeys(cls, iterable, v=None):
        raise NotImplementedError(
            'Method is undefined in context of counter.'
            'Use cls(iterable) instead.')

    @classmethod
    def _to_kv_iterable(cls, iterable):
        if isinstance(iterable, collections.Mapping):
            for key, count in iterable.iteritems():
                if not isinstance(key, basestring):
                    raise ValueError("Key must be instance od basestring")
                yield (key, int(count))
        elif isinstance(iterable, basestring):
            for key, count in collections.Counter(iterable).iteritems():
                yield (key, count)
        else:
            for nested_iterable in iterable:
                nested_list = list(nested_iterable)  # easy way
                if len(nested_list) != 2:
                    raise ValueError(
                        "Nested iterable: %s has length diffrent from 2")
                if not isinstance(nested_list[0], basestring):
                    raise ValueError("Key must be instance od basestring")
                yield (nested_list[0], int(nested_list[1]))

    def __init__(
            self, initial=None, client=None,
            iterable=None, redis_key=None, **kwargs):
        super(MultiSet, self).__init__(
            initial=initial, client=client, redis_key=redis_key)
        self.update(iterable=iterable, **kwargs)

    def __len__(self):
        return self.zcard()

    def __getitem__(self, key):
        if not isinstance(key, basestring):
            raise ValueError("Key must be instance od basestring")
        val = self.zscore(key)
        if val is None:
            return self.__missing__(key)
        return val

    def __iter__(self):
        keys = self.zrange(0, -1)
        for key in keys:
            yield key

    def __setitem__(self, key, value):
        if not isinstance(key, basestring):
            raise ValueError("Key must be instance od basestring")
        self.zadd(key, int(value))

    def __delitem__(self, key):
        self.zrem(key)

    __add__ = op_left(operator.add)
    __sub__ = op_left(operator.sub)
    __and__ = op_left(operator.and_)
    __or__ = op_left(operator.or_)
    __radd__ = op_right(operator.add)
    __rsub__ = op_right(operator.sub)
    __rand__ = op_right(operator.and_)
    __ror__ = op_right(operator.or_)
    __iadd__ = inplace("update")
    __isub__ = inplace("subtract")
    __iand__ = inplace("intersection_update")
    __ior__ = inplace("union_update")

    @property
    def value(self):
        return collections.Counter(self.most_common())

    @value.setter
    def value(self, value):
        if not isinstance(value, dict):
            try:
                value = dict(value)
            except TypeError:
                value = None
        if value:
            self.update(value)

    def __repr__(self):
        bits = (self.__class__.__name__, repr(dict(self.value)), self.key)
        return "%s(%s, '%s')" % bits

    def __missing__(self, key):
        return 0

    def most_common(self, n=None):
        if n == 0:
            return []
        if n is None:
            n = -1
        return self.zrange(0, int(n), True, True, int)

    def elements(self):
        return chain.from_iterable(
            (repeat(key, count) for key, count in self.most_common()))

    def update(self, iterable=None, **kwds):
        iterables = []
        if iterable is not None:
            iterables.append(self._to_kv_iterable(iterable))
        if kwds:
            iterables.append(self._to_kv_iterable(kwds))
        if iterables:
            kvs = list(chain.from_iterable(iterables))
            with self.pipeline() as pipe:
                for key, value in kvs:
                    pipe.zincrby(self.key, value)

    def subtract(self, iterable=None, **kwds):
        iterables = []
        if iterable is not None:
            iterables.append(self._to_kv_iterable(iterable))
        if kwds:
            iterables.append(self._to_kv_iterable(kwds))
        if iterables:
            kvs = list(chain.from_iterable(iterables))
            with self.pipeline(transaction=False) as pipe:
                for key, value in kvs:
                    pipe.zincrby(key, -value)

    def copy(self):
        return self.__class__(self.values)


collections.MutableMapping.register(MultiSet)
