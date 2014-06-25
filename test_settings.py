# -*- coding: utf8 -*-
import os


ROOT = os.path.abspath(os.path.dirname(__file__))

HOT_REDIS = {
    'host': 'localhost',
    'port': 6379,
    # we don't want to mess with your db
    'db': 14
}

REDIS_TEST = True

SECRET_KEY = 'super_secret'

DATABASES = {
    'default': {
        'NAME': ':memory:',
        'ENGINE': 'django.db.backends.sqlite3'
    }
}

REDIS_TEST_PREFIX = '__test__'

INSTALLED_APPS = [
    'hot_redis.contrib.django'
]
