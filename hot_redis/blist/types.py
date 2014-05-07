# -*- coding: utf-8 -*-
import collections
from itertools import chain
import cPickle as pc

import blist
from redis import WatchError

from hot_redis import Base
from hot_redis.conf import PICKLE_PROTOCOL


class SortedSet(collections.Sequence, collections.MutableSet, Base):
    def __getitem__(self, i):
        if not isinstance(i, slice):
            return pc.loads(self.zrange(i, i, desc=True)[0])
        if i.step:
            raise NotImplementedError("step not supported")

        start = i.start if i.start is not None else 0
        stop = i.stop if i.stop is not None else 0
        return [
            pc.loads(instance)
            for instance in self.zrange(start, stop - 1, desc=True)]

    def __len__(self):
        return self.zcard()

    def __delitem__(self, index):
        with self.pipeline() as pipe:
            while 1:
                try:
                    pipe.watch(self.key)
                    value_raw = pipe.zrange(self.key, index, index)[0]
                    if value_raw is None:
                        raise IndexError()
                    pipe.multi()
                    pipe.zrem(self.key, value_raw)
                    pipe.execute()
                    break
                except WatchError:
                    continue

    def __iter__(self):
        if self._eager_iterator:
            values = (pc.loads(pickled_value)
                      for pickled_value in self.zrange(0, -1, desc=True))
        else:
            values = super(SortedSet, self).__iter__()
        for value in values:
            yield value


    def __init__(self, iterable=None, key=None, eager_iterator=True, **kwargs):
        super(SortedSet, self).__init__(**kwargs)
        self._eager_iterator = eager_iterator
        self._key_producer = key or (lambda x: x)
        if iterable:
            self.update(iterable)

    def _get_key(self, value):
        return float(self._key_producer(value))

    def add(self, value):
        self.zadd(**{pc.dumps(value, PICKLE_PROTOCOL): self._get_key(value)})

    def discard(self, value):
        self.zrem(pc.dumps(value, PICKLE_PROTOCOL))

    def update(self, *iterables):
        chain.from_iterable(iterables)
        kwargs = {pc.dumps(value, PICKLE_PROTOCOL): self._get_key(value)
                  for value in chain.from_iterable(iterables)}
        self.zadd(**kwargs)

    @property
    def value(self):
        return blist.sortedset(
            (pc.loads(value) for value in self.zrange(0, -1)),
            self._key_producer)

    @value.setter
    def value(self, value):
        self.update(value)
