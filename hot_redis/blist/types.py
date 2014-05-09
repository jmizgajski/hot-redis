# -*- coding: utf-8 -*-
import collections
from itertools import chain
import cPickle as pc

import blist
from redis import WatchError

from hot_redis import Base
from hot_redis.conf import PICKLE_PROTOCOL


class SortedSet(collections.Sequence, collections.MutableSet, Base):
    def serialize(self, item):
        return pc.dumps(item, PICKLE_PROTOCOL)

    def deserialize(self, serialized_item):
        return pc.loads(serialized_item)

    def __getitem__(self, i):
        if not isinstance(i, slice):
            return self.deserialize(self.zrange(i, i)[0])
        if i.step:
            raise NotImplementedError("step not supported")

        start = i.start if i.start is not None else 0
        stop = i.stop if i.stop is not None else 0
        return [
            self.deserialize(instance)
            for instance in self.zrange(start, stop - 1)]

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
            values = (self.deserialize(pickled_value)
                      for pickled_value in self.zrange(0, -1))
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

    def _to_keys(self, sets):
        return [s.key for s in sets]

    def add(self, value):
        self.zadd(**{self.serialize(value): self._get_key(value)})

    def discard(self, value):
        self.zrem(self.serialize(value))

    def update(self, *iterables):
        chain.from_iterable(iterables)
        kwargs = {self.serialize(value): self._get_key(value)
                  for value in chain.from_iterable(iterables)}
        self.zadd(**kwargs)

    def intersection_update(self, set):
        keys = [self.key, set]
        self.sorted_set_intersection_update(set.key)
        return self

    @property
    def value(self):
        return blist.sortedset(
            (self.deserialize(value) for value in self.zrange(0, -1)),
            self._key_producer)

    @value.setter
    def value(self, value):
        self.update(value)
