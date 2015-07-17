# -*- coding: utf8 -*-
from hot_redis.core.hot_client import HotClient as BaseHotClient


class HotRedisNotConfiguredException(Exception):
    pass


class HotClient(object):
    _client = None

    def __new__(cls):
        if not cls._client:
            try:
                from django.conf import settings
            except ImportError:
                cls._client = BaseHotClient()
            else:
                redis_conf = getattr(settings, 'HOT_REDIS', None)
                if redis_conf is None:
                    raise HotRedisNotConfiguredException
                cls._client = BaseHotClient(
                    **redis_conf
                )
        return cls._client


def get_redis_connection():
    return HotClient()
