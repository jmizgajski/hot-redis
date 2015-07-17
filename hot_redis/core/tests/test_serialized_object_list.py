# -*- coding: utf8 -*-
from datetime import datetime

from hot_redis.core.connection import get_redis_connection
from hot_redis.contrib.django.mappings import ModelMappingType
from hot_redis.contrib.django.case import ClearRedisTestCase
from hot_redis.utils import make_key
from hot_redis.core.types import SerializedObjectList


class SomeClass(object):
    def __init__(self, _id, date):
        super(SomeClass, self).__init__()
        self.id = _id
        self.date = date


class SomeMapping(ModelMappingType):
    class Meta(object):
        model = SomeClass
        fields = ('id', 'date')


class TestSerializedObjectList(ClearRedisTestCase):
    def setUp(self):
        self.client = get_redis_connection()

        self.mapping = SomeMapping

        self.list = [SomeClass(x % 5, datetime.now()) for x in xrange(0, 10)]

        self.object_list = SerializedObjectList(
            serializer=SomeMapping,
            redis_key=make_key('ObjectList'),
            client=self.client
        )
        self.object_list.extend(self.list)

    def test_index(self):
        element = self.list[3]

        self.assertEquals(
            self.object_list.index(element),
            self.list.index(element)
        )

    def test_count(self):

        element_to_count = self.list[1]

        self.assertEquals(
            self.object_list.count(element_to_count),
            self.list.count(element_to_count)
        )
