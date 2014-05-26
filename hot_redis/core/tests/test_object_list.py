# -*- coding: utf8 -*-
from common.lib.redis.contrib.django.connection import get_redis_connection
from common.lib.redis.contrib.django.case import ClearRedisTestCase
from common.lib.redis.contrib.django.utils import make_key
from common.lib.redis.types import ObjectList


class TestObjectList(ClearRedisTestCase):
    def setUp(self):
        self.client = get_redis_connection()

        self.list = [{1: 2}, {1: 3}, {1: 1}, {1: -1}]

        self.object_list = ObjectList(
            redis_key=make_key('ObjectList'),
            client=self.client
        )
        self.object_list.extend(self.list)

    def test_sort_by_key(self):
        self.object_list.sort_by_key(lambda x: x[1])
        sorted_list = sorted(self.list, key=lambda x: x[1])

        self.assertEquals(
            list(self.object_list), sorted_list)

    def test___setitem__(self):

        self.object_list[1] = {1: 15}
        self.list[1] = {1: 15}

        self.assertEquals(list(self.object_list), self.list)

    def test___getitem__(self):

        for i in xrange(0, len(self.list)):
            self.assertEquals(self.object_list[i], self.list[i])

    def test_append(self):

        self.object_list.append({1: 15})
        self.list.append({1: 15})

        self.assertEquals(list(self.object_list), self.list)

    def test_extend(self):
        extension = [{1: 15}, {1: 13}]
        self.object_list.extend(extension)
        self.list.extend(extension)

        self.assertEquals(list(self.object_list), self.list)

    def test_insert(self):
        self.object_list.insert(3, {1: 15})
        self.list.insert(3, {1: 15})

        self.assertEquals(list(self.object_list), self.list)

    def test_test_pop(self):
        x = self.object_list.pop(3)
        x2 = self.list.pop(3)

        self.assertEquals(list(self.object_list), self.list)
        self.assertEquals(x, x2)

        x = self.object_list.pop(-1)
        x2 = self.list.pop(-1)

        self.assertEquals(list(self.object_list), self.list)
        self.assertEquals(x, x2)
