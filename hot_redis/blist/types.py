# -*- coding: utf-8 -*-
import collections
from itertools import chain
import json
import blist
from redis import WatchError

from hot_redis import Base


class SortedSet(collections.Sequence, collections.MutableSet, Base):
    # TODO eager vs lazy __iter__?
    @staticmethod
    def _encode(value):
        return float(
            ''.join((format(ord(x), '03d') for x in json.dumps(value))))

    @staticmethod
    def _decode(raw_value):
        raw_value = raw_value.split('.')[0]
        if len(raw_value) % 3 != 0:
            raw_value = '0' + raw_value
        dumped = ''.join((chr(raw_value[x:x+3])
                          for x in xrange(0, len(raw_value), 3)))
        return json.loads(dumped)

    def __getitem__(self, index):
        if isinstance(index, slice):
            raise NotImplementedError("Slicing not implemented")
        else:
            return json.loads(self.zrange(index, index)[0])

    def __len__(self):
        return self.zcard()

    def __init__(self, iterable=None, key=None, **kwargs):
        super(SortedSet, self).__init__(**kwargs)
        self._key_producer = key or (lambda x: x)
        if iterable:
            self.update(iterable)

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

    def _get_key(self, value):
        return float(self._key_producer(value))

    def add(self, value):
        self.zadd(**{json.dumps(value): self._get_key(value)})

    def discard(self, value):
        self.zrem(json.dumps(value))

    def update(self, *iterables):
        chain.from_iterable(iterables)
        kwargs = {json.dumps(value): self._get_key(value)
                  for value in chain.from_iterable(iterables)}
        self.zadd(**kwargs)

    @property
    def value(self):
        return blist.sortedset(
            (json.loads(value) for value in self.zrange(0, -1)),
            self._key_producer)

    @value.setter
    def value(self, value):
        self.update(value)
