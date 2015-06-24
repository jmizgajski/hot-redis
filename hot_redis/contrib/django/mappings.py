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
        
        
# TODO: write comprehensive tests - this is tested only in production with 
# internal tests
class LightweightModelMappingType(ModelMappingType):
    """
    This is a lightweight (in terms of Redis memory footprint) but more verbose
    alternative to ModelMappingType. Key assumption:

    - fields are serialized in order they are provided in meta with str method

    - example:

        class A(LightweightModelMappingType):
            class Meta(object):
                fields = ('id', 'other_id')
                deserializers = (int, int)

        When given a model with id=12 and other_id=44 will serialize to "12;44"

    - order of fields in Meta.fields matters

    - order of deserializers in Meta.deserializers matters and should
    correspond to the the order of Meta.fields

    - if None is provided for a deserializer the output type will be string

    - use Extractor class to prepare field for serialization - for example
    you can convert datetimes to unix time (saves space) and provide a
    deserializer that will parse the unix time and convert back to datetime

    - your fields MUST NOT contain the SEPARATOR character

    - if you change the order of fields in production you have to recreate your
    Redis database or convert it to new format

    - Keys will be serialized in order in which they are defined in meta
    Changing an order of fields in Meta without recreation will cause errors!

    - bonus: fields are readable to other programming languages

    """
    SEPARATOR = ";"

    class Meta(object):
        model = None
        fields = ('id',)
        deserializers = (int,)

    @classmethod
    def serialize(cls, obj):
        prepared = cls.prepare(obj)
        return cls.SEPARATOR.join(
            [str(prepared[field]) for field in cls.Meta.fields])

    @classmethod
    def wrap(cls, dictionary):
        tpl = namedtuple(
            "%sCache" % cls.Meta.model.__name__,
            cls.Meta.fields
        )
        return tpl(**dictionary)

    @classmethod
    def deserialize(cls, serialized):
        if isinstance(serialized, str):
            return cls.wrap(dict(imap(
                lambda (f, d, v): (f, d(v)) if d else (f, v),
                izip(
                    cls.Meta.fields,
                    cls.Meta.deserializers,
                    serialized.split(cls.SEPARATOR))
                )))
