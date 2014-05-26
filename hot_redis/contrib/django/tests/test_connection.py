# -*- coding: utf8 -*-
from django.utils.unittest.case import TestCase
from django.conf import settings

class TestConnection(TestCase):

    def test_get_connection_from_settings(self):
        from hot_redis.contrib.django.connection import (
            get_redis_connection, HotClient)

        redis = get_redis_connection()
        self.assertTrue(redis)
        redis = HotClient()
        self.assertTrue(redis)
