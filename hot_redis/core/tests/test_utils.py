# -*- coding: utf8 -*-
from unittest import TestCase

from hot_redis.core.connection import get_redis_connection

from hot_redis.utils import delete_by_pattern


class TestDeleteByPattern(TestCase):
    def test_delete(self):
        redis = get_redis_connection()
        ''':type: redis.Redis  '''

        keys_that_match = ['test:1:tset', 'test:2:tset']
        keys_that_dont_match = ['head', 'colour:2:tset', 'test:1:head']
        val = 'test'

        for key in keys_that_match + keys_that_dont_match:
            redis.set(key, val)
        for key in keys_that_match + keys_that_dont_match:
            self.assertEquals(redis.get(key), val)

        delete_by_pattern("test:*:tset", redis)

        for key in keys_that_match:
            self.assertEquals(redis.get(key), None)

        for key in keys_that_dont_match:
            self.assertEquals(redis.get(key), val)

        # cleanup
        redis.delete(*(keys_that_match + keys_that_dont_match))
