# -*- coding: utf8 -*-
from unittest import TestCase
from hot_redis.contrib.django import mappings
from hot_redis.contrib.django.mappings import ModelMappingType


class TestMappings(TestCase):
    def test_extracting_simple_fields(self):

        class SampleMapping(ModelMappingType):
            class Meta(object):
                fields = ('id', 'user')

        class SampleModel(object):
            id = 1
            user = 'czesław'

        self.assertEquals(
            {'id': 1, 'user': 'czesław'},
            SampleMapping.prepare(SampleModel())
        )

    def test_extracting_simple_and_complex_fields(self):

        class SampleMapping(ModelMappingType):
            user = mappings.Extractor(lambda obj: obj.user + "_1")

            class Meta(object):
                fields = ('id', 'user')

        class SampleModel(object):
            id = 1
            user = 'czesław'

        self.assertEquals(
            {'id': 1, 'user': 'czesław_1'},
            SampleMapping.prepare(SampleModel())
        )

    def test_extracting_when_no_fields_in_meta(self):

        class SampleMapping(ModelMappingType):
            user = mappings.Extractor(lambda obj: obj.user + "_1")
            #will not be serialized - is not an extractor or a string
            id = 1

            class Meta(object):
                pass

        class SampleModel(object):
            id = 1
            user = 'czesław'

        self.assertEquals(
            {'user': 'czesław_1'},
            SampleMapping.prepare(SampleModel())
        )
