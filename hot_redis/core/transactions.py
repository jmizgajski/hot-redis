# -*- coding: utf8 -*-
from functools import partial, wraps
from itertools import izip
from hot_redis.core.tests.test_utils import get_redis_connection


def decorated_transaction_wrapper(watched, instant_multi, func):
    redis = get_redis_connection()
    ''':type :redis.client.Redis'''

    @wraps(func)
    def transaction_wrapper(*args, **kwargs):
        result = []

        def transaction(pipe):
            old_clients = []
            for piped_object in watched:
                old_clients.append(piped_object.client)
                piped_object.client = pipe
            try:
                if instant_multi:
                    pipe.multi()
                    result.append(func(*args, **kwargs))
                else:
                    result.append(func(*args, pipe=pipe, **kwargs))
            finally:
                for piped_object, old_client in izip(watched, old_clients):
                    piped_object.client = old_client

        watched_keys = [x.key for x in watched]
        redis.transaction(
            transaction,
            *watched_keys,
            multi=(len(watched_keys) == 0)
        )
        return result.pop()
    return transaction_wrapper


def transaction(*watched):
    return partial(decorated_transaction_wrapper, watched, False)


def multi(*watched):
    return partial(decorated_transaction_wrapper, watched, True)
