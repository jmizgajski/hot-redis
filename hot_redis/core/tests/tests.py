# -*- coding: utf-8 -*-

import unittest
from hot_redis.utils import make_key
from hot_redis.core.tests.test_utils import get_redis_connection
from hot_redis.utils import DEFAULT_TEST_PREFIX
from hot_redis.utils import delete_by_pattern
from hot_redis import core, ObjectList

keys = []

# FIXME: [To discuss] export lua multi methods to a subclass
# HotAnalyticsClient (which could be kept inside moodly)
# FIXME: split tests into individual files
# FIXME: SerializedSortedObjectSet


def base_wrapper(init):
    def wrapper(*args, **kwargs):
        init(*args, **kwargs)
        keys.append(args[0].key)

    return wrapper


core.Base.__init__ = base_wrapper(core.Base.__init__)


class BaseTestCase(unittest.TestCase):
    @staticmethod
    def clear_prefixed_keys():
        client = get_redis_connection()
        pattern = "%s*" % (DEFAULT_TEST_PREFIX)
        delete_by_pattern(pattern, client)

    def setUp(self):
        self.clear_prefixed_keys()

    def tearDown(self):
        client = core.default_client()
        while keys:
            client.delete(keys.pop())
