# -*- coding: utf8 -*-
from abc import ABCMeta, abstractmethod
import cPickle as pc
from collections import namedtuple
import json

from django.core import serializers
from django.core.serializers.base import DeserializationError
from django.db.models import get_model
from django.db.models.signals import (
    pre_init, post_init, pre_save, post_save, pre_delete, post_delete)


class Extractor(object):
    """
    If you have a field that is not directly accessible as an attribute of
    the model you are trying to serialize such as ``model_1.model_2.id``
    create an ``Extractor`` and pass a function that will take the
    ``model_1`` instance as argument and return the field you need.
    """

    def __init__(self, extractor):
        self.extractor = extractor

    def __call__(self, obj):
        return self.extractor(obj)


class MappingType(object):
    __metaclass__ = ABCMeta

    @classmethod
    @abstractmethod
    def serialize(cls, value):
        """

        :param value:
        """

    @classmethod
    @abstractmethod
    def deserialize(cls, string):
        """

        :param string:
        """


class DjangoModelMapping(MappingType):
    def __init__(
            self,
            model_class,
            fields=None,
            pre_init_hook=None,
            post_init_hook=None,
            pre_save_hook=None,
            post_save_hook=None,
            pre_delete_hook=None,
            post_delete_hook=None):
        super(DjangoModelMapping, self).__init__()
        self.model_class = model_class
        fields_set = set(fields) if fields else set(
            field.name for field in model_class._meta.fields)
        fields_set.add(self.model_class._meta.pk.name)
        self.fields = list(fields_set)
        self.tpl = namedtuple(
            "%sCache" % self.model_class.__name__,
            self.fields
        )
        if pre_init_hook:
            pre_init.connect(pre_init_hook, model_class, weak=False)
        if post_init_hook:
            post_init.connect(post_init_hook, model_class, weak=False)
        if pre_save_hook:
            pre_save.connect(pre_save_hook, model_class, weak=False)
        if post_save_hook:
            post_save.connect(post_save_hook, model_class, weak=False)
        if pre_delete_hook:
            pre_delete.connect(pre_delete_hook, model_class, weak=False)
        if post_delete_hook:
            post_delete.connect(post_delete_hook, model_class, weak=False)

    def deserialize(self, string):
        json_object = json.loads(string)
        fullmodelname = json_object["model"].split('.', 1)
        if len(fullmodelname) >= 3:
            raise DeserializationError("Incorrect model")
        model = get_model(fullmodelname[0], fullmodelname[1])
        if model != self.model_class:
            raise TypeError(
                "Incorrect serialized model class. Expected: %s, got: %s"
                % (self.model_class, model))
        fields_values = json_object['fields']
        for field in self.model_class._meta.fields:
            if field.name in fields_values:
                fields_values[field.name] = field.to_python(
                    fields_values[field.name])
        if self.model_class._meta.pk.name not in fields_values:
            fields_values[self.model_class._meta.pk.name] = json_object["pk"]
        return self.tpl(**fields_values)

    def serialize(self, value):
        if not isinstance(value, self.model_class):
            raise TypeError(
                "Incorrect serializer, value type: %s, serializer type: %s"
                % (value.__class__, self.model_class))
        return serializers.serialize(
            "json", [value], fields=self.fields)[1:-1]


class ModelMappingType(MappingType):
    PICKLE_PROTOCOL = 2
    _wrapper_class = None

    class Meta(object):
        model = None
        fields = ('id',)

    @classmethod
    def prepare(cls, obj):
        to_serialize = {}
        if getattr(cls.Meta, 'fields', tuple()):
            for field in cls.Meta.fields:
                extractor = getattr(cls, field, None)
                ''':type: Extractor'''
                if extractor:
                    to_serialize[field] = extractor(obj)
                else:
                    to_serialize[field] = getattr(obj, field)
        else:
            mapping_fields = {
                k: v
                for k, v
                in vars(cls).iteritems()
                if (isinstance(v, Extractor) or
                    isinstance(v, basestring) and not k.startswith('_'))
            }
            for field_name, extractor in mapping_fields.iteritems():
                if isinstance(extractor, Extractor):
                    to_serialize[field_name] = extractor(obj)
                else:
                    to_serialize[field_name] = getattr(obj, extractor)
        return to_serialize

    @classmethod
    def serialize(cls, obj):
        # Warning: use of pickle as serializer might cause errors
        # deserialized object might be different than serialized, when class
        # definition changed over time.
        # In our case it is very unlikely, but still it should be considered.
        return pc.dumps(cls.prepare(obj), protocol=cls.PICKLE_PROTOCOL)

    @classmethod
    def wrap(cls, dictionary):
        if not cls._wrapper_class:
            cls._wrapper_class = namedtuple(
                "%sCache" % cls.Meta.model.__name__,
                cls.Meta.fields
            )
        return cls._wrapper_class(**dictionary)

    @classmethod
    def deserialize(cls, pickle):
        return cls.wrap(pc.loads(pickle))
