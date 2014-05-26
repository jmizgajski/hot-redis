# -*- coding: utf8 -*-
from django.conf import settings

DEFAULT_TEST_PREFIX = '__test__'
KEY_SEPARATOR = '.'


def make_key(*parts):
    key = KEY_SEPARATOR.join(str(part) for part in parts)

    if getattr(settings, 'REDIS_TEST', False):
        prefix = getattr(
            settings, 'REDIS_TEST_KEY_PREFIX', DEFAULT_TEST_PREFIX)
        return "%s%s" % (prefix, key)

    return key
