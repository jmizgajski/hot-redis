# -*- coding: utf-8 -*-
from django.conf import settings
from django.test import TransactionTestCase


class ClearRedisTestCase(TransactionTestCase):
    @staticmethod
    def clear_prefixed_keys():
        client = get_redis_connection()
        #clear all testable keys
        pattern = "%s*" % getattr(
            settings,
            'REDIS_TEST_KEY_PREFIX',
            DEFAULT_TEST_PREFIX
        )
        delete_by_pattern(pattern, client)

    def _pre_setup(self):
        super(ClearRedisTestCase, self)._pre_setup()
        self.clear_prefixed_keys()
