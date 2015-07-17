# -*- coding: utf8 -*-
from hot_redis.contrib.django.case import ClearRedisTestCase
from hot_redis.core.connection import get_redis_connection
from hot_redis.utils import make_key
from hot_redis.core.transactions import multi, transaction
from hot_redis import List


class TestRedisMulti(ClearRedisTestCase):
    def test_rollback(self):
        x = List(client=get_redis_connection(),
                 redis_key=make_key('_test_list_key'))
        y = List(client=get_redis_connection(),
                 redis_key=make_key('_test_list_key_2'))

        @multi(x, y)
        def some_transaction_func(x, y, raise_error=True):
            y.append(1)
            x.append(1)
            if raise_error:
                raise ValueError()
            x.append(2)
            return 777

        return_value = 0
        with self.assertRaises(ValueError):
            return_value = some_transaction_func(x, y, raise_error=True)

        self.assertEquals(len(x), 0)
        self.assertEquals(len(y), 0)
        self.assertEquals(return_value, 0)

        return_value = some_transaction_func(x, y, raise_error=False)

        self.assertEquals(len(x), 2)
        self.assertEquals(len(y), 1)
        self.assertEquals(return_value, 777)


class TestRedisTransaction(ClearRedisTestCase):
    def test_rollback(self):
        x = List(client=get_redis_connection(), redis_key=make_key('list_key'))
        y = List(client=get_redis_connection(),
                 redis_key=make_key('list_key_2'))

        @transaction(x, y)
        def some_transaction_func(x, y, pipe, raise_error=True):
            pipe.multi()
            y.append(1)
            x.append(1)
            if raise_error:
                raise ValueError()
            x.append(2)
            return 777

        return_value = 0
        try:
            return_value = some_transaction_func(x, y, raise_error=True)
        except ValueError:
            pass

        self.assertEquals(len(x), 0)
        self.assertEquals(len(y), 0)
        self.assertEquals(return_value, 0)

        return_value = some_transaction_func(x, y, raise_error=False)

        self.assertEquals(len(x), 2)
        self.assertEquals(len(y), 1)
        self.assertEquals(return_value, 777)
