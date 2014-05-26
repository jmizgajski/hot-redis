# -*- coding: utf8 -*-
from abc import ABCMeta, abstractmethod
import cPickle as pc
from collections import namedtuple


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


class ModelMappingType(MappingType):
    PICKLE_PROTOCOL = 2

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
                if isinstance(v, Extractor) or (
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
        tpl = namedtuple(
            "%sCache" % cls.Meta.model.__name__,
            cls.Meta.fields
        )
        return tpl(**dictionary)

    @classmethod
    def deserialize(cls, pickle):
        return cls.wrap(pc.loads(pickle))
