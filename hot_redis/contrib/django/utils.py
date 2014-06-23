# -*- coding: utf8 -*-
from django.conf import settings
from hot_redis.utils import KEY_SEPARATOR, DEFAULT_TEST_PREFIX

def make_key(*parts):
    key = KEY_SEPARATOR.join(str(part) for part in parts)

    return key


def prefix_key(key):
    if getattr(settings, 'REDIS_TEST', False):
        prefix = getattr(
            settings, 'REDIS_TEST_KEY_PREFIX', DEFAULT_TEST_PREFIX)
        if key.find(prefix) == 0:
            return key
        else:
            return "%s%s%s" % (prefix, KEY_SEPARATOR,  key)

    return key
