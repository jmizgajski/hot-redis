# -*- coding: utf8 -*-
from abc import ABCMeta
from hot_redis.utils import make_key, get_instance_id
from hot_redis.contrib.django.connection import get_redis_connection
from hot_redis import hot_redis, types



class Field(object):
    __metaclass__ = ABCMeta
    _key_base = None
    _name = None
    _shared_field = None
    _client = None
    _hot_redis_type = None

    @property
    def client(self):
        """

        :return: a redis connection from redis_cache connection pool or one
        supplied by the user via the setter
        :rtype: redis.client.Redis
        """
        if self._client:
            return self._client

        self._client = get_redis_connection()
        return self._client

    @client.setter
    def client(self, new_client):
        self._client = new_client

    def __init__(self, shared_field=False, *type_args, **type_kwargs):

        self._shared_field = shared_field
        self._type_args = type_args
        self._type_kwargs = type_kwargs

    def _get_key(self, instance):
        """
        e.g 'common.models.TrackedUser.56.viewed_items'
        or  'common.utils.Dudud.1.unknown_field_6f422984-4d89-4b10...'
        or (shared_field) 'common.utils.Dudud.unknown_field_6f422984-4d...'

        :param instance:
        :return: redis key
        """
        if not self._shared_field:
            try:
                instance_id_string = get_instance_id(instance)
                return make_key(
                    self._key_base,
                    instance_id_string,
                    self._name
                )
            except ValueError:
                raise ValueError("Instance should have 'id' or 'pk' field")

    # Invoked by django model metaclasses
    def contribute_to_class(self, cls, name, virtual_only=False):
        self._key_base = get_class_fqn(cls)
        self._name = name
        setattr(cls, name, self)

    def __get__(self, instance, owner):

        if not self._key_base:
            self._key_base = self.make_key_base(instance.__class__)

        return self._hot_redis_type(
            redis_key=self._get_key(instance),
            client=self.client,
            *self._type_args,
            **self._type_kwargs
        )

    def __set__(self, instance, value):
        if not self._key_base:
            self._key_base = self.make_key_base(instance.__class__)
        key = self._get_key(instance)
        self.client.delete(key)
        self._hot_redis_type(
            redis_key=key,
            client=self.client,
            initial=value,
            *self._type_args,
            **self._type_kwargs
        )

    @classmethod
    def make_key_base(cls, model_cls):
        return get_class_fqn(model_cls)


class FieldAccessor(object):
    """
    This class is used to access and manipulate hot_redis types wrapped by
    RedisField without calling the database to get the model instance
    """
    # TODO: we could add a method that would take just Model.field and model_id

    @staticmethod
    def get_value_by_id(
            field_cls, client,
            model_cls, model_id, field_name,
            *hot_redis_type_args, **hot_redis_type_kwargs):
        if not field_cls._hot_redis_type:
            raise ValueError("A redis based model Field has to have a "
                             "hot_redis_type defined")

        key = make_key(
            field_cls.make_key_base(model_cls),
            model_id,
            field_name
        )

        return field_cls._hot_redis_type(
            redis_key=key,
            client=client,
            *hot_redis_type_args,
            **hot_redis_type_kwargs
        )


class List(Field):
    _hot_redis_type = hot_redis.List


class Counter(Field):
    _hot_redis_type = hot_redis.MultiSet


class String(Field):
    _hot_redis_type = hot_redis.String


class SerializedObjectList(Field):
    _hot_redis_type = types.SerializedObjectList

    def __init__(self, serializer, shared_field=False, *args, **kwargs):
        super(SerializedObjectList, self).__init__(
            shared_field, serializer, *args, **kwargs)


class SortedSerializedObjectSet(Field):
    _hot_redis_type = types.SortedSerializedObjectSet

    def __init__(self, serializer, key, shared_field=False, *args, **kwargs):
        super(SortedSerializedObjectSet, self).__init__(
            shared_field, serializer, key, *args, **kwargs)
