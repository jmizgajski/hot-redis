from tests import BaseTestCase
from hot_redis.contrib.django.utils import make_key, prefix_key

from hot_redis import core


class LuaMultiMethodsTests(BaseTestCase):
    def setUp(self):
        super(LuaMultiMethodsTests, self).setUp()
        self.longMessage = True

    def test_rank_lists_by_length(self):
        lengths = (61., 60., 5., 4., 3., 2., 1.)
        _keys = [("l%d" % length) for length in lengths]
        keys_with_lengths = zip(_keys, lengths)
        test_keys = [prefix_key(make_key(x)) for x in _keys]
        test_keys_with_lengths = zip(test_keys, lengths)

        lists = [
            core.List(range(int(length)), redis_key=key)
            for key, length
            in keys_with_lengths
        ]

        client = core.default_client()

        calls = [
            (lambda i: [x for x in i], 'iter'),
            (lambda i: i[:], '[:]'),
            (lambda i: i[3:4], '[3:4]'),
            (lambda i: i[-4:3], '[-4:3]'),
            (lambda i: i[2:], '[2:]'),
            (lambda i: i[:2], '[:2]'),
            # empty),
            (lambda i: i[2:1], '[2:1]'),
            (lambda i: i[-1:-2], '[-1:-2]')
        ]

        for call, code in calls:
            expected = call(test_keys_with_lengths)
            ranking = client.rank_lists_by_length(*test_keys)
            result = call(ranking)

            self.assertEquals(
                result,
                expected,
                "Results not equal when called with call={code}, "
                "got result={result}, expected={expected}".format(
                    code=code, result=result,
                    expected=expected
                )
            )

        with self.assertRaises(ValueError):
            client.rank_lists_by_length('only_one_key'),

    def test_rank_sets_by_cardinality(self):
        cardinalities = (61., 60., 5., 4., 3., 2., 1.)
        _keys = [("s%d" % card) for card in cardinalities]
        keys_with_cardinalities = zip(_keys, cardinalities)
        test_keys = [prefix_key(make_key(x)) for x in _keys]
        test_keys_with_cardinalities = zip(test_keys, cardinalities)
        sets = [
            core.Set(range(int(card)), redis_key=key)
            for key, card
            in keys_with_cardinalities
        ]
        client = core.default_client()

        calls = [
            (lambda i: [x for x in i], 'iter'),
            (lambda i: i[:], '[:]'),
            (lambda i: i[3:4], '[3:4]'),
            (lambda i: i[-4:3], '[-4:3]'),
            (lambda i: i[2:], '[2:]'),
            (lambda i: i[:2], '[:2]'),
            # empty),
            (lambda i: i[2:1], '[2:1]'),
            (lambda i: i[-1:-2], '[-1:-2]')
        ]

        for call, code in calls:
            expected = call(test_keys_with_cardinalities)
            result = call(client.rank_sets_by_cardinality(*test_keys))

            self.assertEquals(
                result,
                expected,
                "Results not equal when called with call={code}, "
                "got result={result}, expected={expected}".format(
                    code=code, result=result,
                    expected=expected
                )
            )

        with self.assertRaises(ValueError):
            client.rank_sets_by_cardinality('only_one_key'),

    def test_rank_zsets_by_cardinality(self):
        cardinalities = (61., 60., 5., 4., 3., 2., 1.)
        _keys = [("z%d" % card) for card in cardinalities]
        keys_with_cardinalities = zip(_keys, cardinalities)
        test_keys = [prefix_key(make_key(x)) for x in _keys]
        test_keys_with_cardinalities = zip(test_keys, cardinalities)
        client = core.default_client()

        [
            core.MultiSet(
                initial=[(str(x), x) for x in range(int(card))],
                redis_key=key,
                client=client
            )
            for key, card
            in keys_with_cardinalities
        ]

        calls = [
            (lambda i: [x for x in i], 'iter'),
            (lambda i: i[:], '[:]'),
            (lambda i: i[3:4], '[3:4]'),
            (lambda i: i[-4:3], '[-4:3]'),
            (lambda i: i[2:], '[2:]'),
            (lambda i: i[:2], '[:2]'),
            # empty),
            (lambda i: i[2:1], '[2:1]'),
            (lambda i: i[-1:-2], '[-1:-2]')
        ]

        for call, code in calls:
            expected = call(test_keys_with_cardinalities)
            result = call(client.rank_zsets_by_cardinality(*test_keys))
            self.assertEquals(
                result,
                expected,
                "Results not equal when called with call={code}, "
                "got result={result}, expected={expected}".format(
                    code=code, result=result,
                    expected=expected
                )
            )

        with self.assertRaises(ValueError):
            client.rank_zsets_by_cardinality('only_one_key'),

    def test_multi_zset_fixed_width_histogram(self):
        client = core.default_client()

        z1 = core.MultiSet(
            initial=[('a', 9), ('aa', 9), ('aaa', 11)],
            redis_key='z1',
            client=client
        )
        z2 = core.MultiSet(
            initial=[('a', 5), ('aa', 12)],
            redis_key='z2',
            client=client
        )

        result = client.multi_zset_fixed_width_histogram(
            7, 30, 10, z2.key, z1.key)

        print result
        self.assertEquals(sorted(result), sorted([(0L, 2.0), (10L, 2.0)]))
