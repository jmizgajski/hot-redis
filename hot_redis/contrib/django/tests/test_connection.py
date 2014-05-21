# -*- coding: utf8 -*-
from django.utils.unittest.case import TestCase


class TestConnection(TestCase):
    def test_get_connection_from_settings(self):
        from contrib.django.connection import (
            get_redis_connection, HotClient)

        redis = get_redis_connection()
        self.assertTrue(redis)
        redis = HotClient()
        self.assertTrue(redis)