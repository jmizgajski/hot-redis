# -*- coding: utf-8 -*-
import collections
import time
import Queue
import unittest
from hot_redis.utils import make_key
from hot_redis.core.tests.test_utils import get_redis_connection
from hot_redis.utils import DEFAULT_TEST_PREFIX
from hot_redis.utils import delete_by_pattern
from hot_redis import core, ObjectList

keys = []

# FIXME: [To discuss] export lua multi methods to a subclass
# HotAnalyticsClient (which could be kept inside moodly)
# FIXME: split tests into individual files
# FIXME: SerializedSortedObjectSet


def base_wrapper(init):
    def wrapper(*args, **kwargs):
        init(*args, **kwargs)
        keys.append(args[0].key)

    return wrapper


core.Base.__init__ = base_wrapper(core.Base.__init__)


class BaseTestCase(unittest.TestCase):
    @staticmethod
    def clear_prefixed_keys():
        client = get_redis_connection()
        pattern = "%s*" % (DEFAULT_TEST_PREFIX)
        delete_by_pattern(pattern, client)

    def setUp(self):
        self.clear_prefixed_keys()

    def tearDown(self):
        client = core.default_client()
        while keys:
            client.delete(keys.pop())

class ListTests(BaseTestCase):
    def test_value(self):
        a = ["wagwaan", "hot", "skull"]
        self.assertEquals(core.List(a), a)

    def test_empty(self):
        self.assertEquals(core.List(), [])

    def test_iter(self):
        a = ["wagwaan", "hot", "skull"]
        for i, x in enumerate(core.List(a)):
            self.assertEquals(x, a[i])

    def test_add(self):
        a = ["wagwaan", "hot", "skull"]
        b = ["nba", "hang", "time"]
        self.assertEquals(a + b, core.List(a) + core.List(b))
        self.assertEquals(a + b, core.List(a) + b)
        c = core.List(a)
        d = core.List(b)
        d += c
        c += b
        self.assertEquals(a + b, c)
        self.assertEquals(b + a, d)

    def test_mul(self):
        a = ["wagwaan", "hot", "skull"]
        b = core.List(a)
        i = 9000
        self.assertEquals(a * i, core.List(a) * i)
        b *= i
        self.assertEquals(a * i, b)

    def test_len(self):
        a = ["wagwaan", "hot", "skull"]
        self.assertEquals(len(a), len(core.List(a)))

    def test_get(self):
        a = ["wagwaan", "hot", "skull"] * 10
        b = core.List(a)
        self.assertEquals(a[4], b[4])
        self.assertEquals(a[3:12], b[3:12])
        self.assertEquals(a[:-5], b[:-5])
        self.assertRaises(IndexError, lambda: b[len(b)])

    def test_set(self):
        a = ["wagwaan", "hot", "skull"]
        b = core.List(a)
        i = "popcaan"
        a[1] = i
        self.assertNotEquals(a, b)
        b[1] = i
        self.assertEquals(a, b)
        # todo: slice

    def test_del(self):
        a = ["wagwaan", "hot", "skull"]
        b = core.List(a)
        del a[1]
        self.assertNotEquals(a, b)
        del b[1]
        self.assertEquals(a, b)
        # todo: slice?

    def test_len(self):
        a = ["wagwaan", "hot", "skull"]
        b = core.List(a)
        self.assertEquals(len(a), len(b))

    def test_contains(self):
        a = ["wagwaan", "hot", "skull"]
        b = core.List(a)
        self.assertIn("wagwaan", a)
        self.assertNotIn("hotskull", a)

    def test_extend(self):
        a = ["wagwaan", "hot", "skull"]
        b = ["nba", "hang", "time"]
        c = core.List(a)
        a.extend(b)
        c.extend(b)
        self.assertEquals(a, c)

    def test_append(self):
        a = ["wagwaan", "hot", "skull"]
        b = core.List(a)
        i = "popcaan"
        a.append(i)
        b.append(i)
        self.assertEquals(a, b)

    def test_insert(self):
        a = ["wagwaan", "hot", "skull"]
        b = core.List(a)
        i = "popcaan"
        a.insert(1, i)
        b.insert(1, i)
        self.assertEquals(a, b)

    def test_pop(self):
        a = ["wagwaan", "hot", "skull"] * 10
        b = core.List(a)
        a.pop()
        b.pop()
        self.assertEquals(a, b)
        a.pop(0)
        b.pop(0)
        self.assertEquals(a, b)
        a.pop(-1)
        b.pop(-1)
        self.assertEquals(a, b)
        a.pop(20)
        b.pop(20)
        self.assertEquals(a, b)

    def test_reverse(self):
        a = ["wagwaan", "hot", "skull"]
        b = core.List(a)
        a.reverse()
        b.reverse()
        self.assertEquals(a, b)

    def test_index(self):
        a = ["wagwaan", "hot", "skull"] * 10
        b = core.List(a)
        c = "wagwaan"
        self.assertEquals(a.index(c), b.index(c))
        self.assertRaises(ValueError, lambda: b.index("popcaan"))

    def test_count(self):
        a = ["wagwaan", "hot", "skull"] * 10
        b = core.List(a)
        self.assertEquals(a.count("wagwaan"), b.count("wagwaan"))
        self.assertEquals(a.count("popcaan"), b.count("popcaan"))

    def test_sort(self):
        a = ["wagwaan", "hot", "skull"] * 10
        b = core.List(a)
        a.sort()
        b.sort()
        self.assertEquals(a, b)
        a.sort(reverse=True)
        b.sort(reverse=True)
        self.assertEquals(a, b)


class SetTests(BaseTestCase):
    def test_value(self):
        a = set(["wagwaan", "hot", "skull"])
        self.assertEquals(core.Set(a), a)

    def test_empty(self):
        self.assertEquals(core.Set(), set())

    def test_add(self):
        a = set(["wagwaan", "hot", "skull"])
        b = core.Set(a)
        i = "popcaan"
        a.add(i)
        b.add(i)
        self.assertEquals(b, a)

    def test_update(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["nba", "hang", "time"])
        c = set(["rap", "dot", "mom"])
        d = core.Set(a)
        a.update(b, c)
        d.update(b, c)
        self.assertEquals(d, a)

    def test_pop(self):
        a = core.Set(["wagwaan", "hot", "skull"])
        i = len(a)
        b = a.pop()
        self.assertEquals(len(a), i - 1)
        self.assertNotIn(b, a)

    def test_clear(self):
        a = core.Set(["wagwaan", "hot", "skull"])
        a.clear()
        self.assertEquals(len(a), 0)

    def test_remove(self):
        a = core.Set(["wagwaan", "hot", "skull"])
        i = len(a)
        b = "wagwaan"
        a.remove(b)
        self.assertEquals(len(a), i - 1)
        self.assertNotIn(b, a)
        self.assertRaises(KeyError, lambda: a.remove("popcaan"))

    def test_discard(self):
        a = core.Set(["wagwaan", "hot", "skull"])
        i = len(a)
        b = "wagwaan"
        a.discard(b)
        self.assertEquals(len(a), i - 1)
        self.assertNotIn(b, a)
        self.assertEquals(a.discard("popcaan"), None)

    def test_len(self):
        a = set(["wagwaan", "hot", "skull"])
        b = core.Set(a)
        self.assertEquals(len(a), len(b))

    def test_contains(self):
        a = core.Set(["wagwaan", "hot", "skull"])
        self.assertIn("wagwaan", a)
        self.assertNotIn("popcaan", a)

    def test_intersection(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = set(["wagwaan", "worldstar", "hiphop"])
        d = core.Set(a)
        e = a.intersection(b, c)
        self.assertEquals(a.intersection(b), d.intersection(b))
        self.assertEquals(e, d.intersection(b, c))
        self.assertEquals(e, d.intersection(core.Set(b), c))
        self.assertEquals(e, d.intersection(b, core.Set(c)))
        self.assertEquals(e,
                          d.intersection(core.Set(b), core.Set(c)))

    def test_intersection_update(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = set(["wagwaan", "worldstar", "hiphop"])
        d = a.copy()
        d.intersection_update(b)
        e = core.Set(a)
        e.intersection_update(b)
        self.assertEquals(e, d)
        d = a.copy()
        d.intersection_update(b, c)
        e = core.Set(a)
        e.intersection_update(b, c)
        self.assertEquals(e, d)
        e = core.Set(a)
        e.intersection_update(core.Set(b), c)
        self.assertEquals(e, d)
        e = core.Set(a)
        e.intersection_update(b, core.Set(c))
        self.assertEquals(e, d)
        e = core.Set(a)
        e.intersection_update(core.Set(b), core.Set(c))
        self.assertEquals(e, d)

    def test_difference(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = set(["wagwaan", "worldstar", "hiphop"])
        d = core.Set(a)
        e = a.difference(b, c)
        self.assertEquals(a.difference(b), d.difference(b))
        self.assertEquals(e, d.difference(b, c))
        self.assertEquals(e, d.difference(core.Set(b), c))
        self.assertEquals(e, d.difference(b, core.Set(c)))
        self.assertEquals(e, d.difference(core.Set(b), core.Set(c)))

    def test_difference_update(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = set(["wagwaan", "worldstar", "hiphop"])
        d = a.copy()
        d.difference_update(b)
        e = core.Set(a)
        e.difference_update(b)
        self.assertEquals(e, d)
        d = a.copy()
        d.difference_update(b, c)
        e = core.Set(a)
        e.difference_update(b, c)
        self.assertEquals(e, d)
        e = core.Set(a)
        e.difference_update(core.Set(b), c)
        self.assertEquals(e, d)
        e = core.Set(a)
        e.difference_update(b, core.Set(c))
        self.assertEquals(e, d)
        e = core.Set(a)
        e.difference_update(core.Set(b), core.Set(c))
        self.assertEquals(e, d)

    def test_symmetric_difference(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = core.Set(a)
        d = a.symmetric_difference(b)
        self.assertEquals(d, c.symmetric_difference(b))
        self.assertEquals(d, c.symmetric_difference(core.Set(b)))
        self.assertEquals(d, a.symmetric_difference(core.Set(b)))

    def test_symmetric_difference_update(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["wagwaan", "flute", "don"])
        c = a.copy()
        c.difference_update(b)
        d = core.Set(a)
        d.difference_update(b)
        self.assertEquals(d, c)
        d = core.Set(a)
        d.difference_update(core.Set(b))
        self.assertEquals(d, c)

    def test_disjoint(self):
        a = set(["wagwaan", "hot", "skull"])
        b = core.Set(a)
        c = core.Set(["wagwaan", "flute", "don"])
        d = set(["nba", "hang", "time"])
        e = core.Set(d)
        self.assertFalse(b.isdisjoint(a))
        self.assertFalse(b.isdisjoint(c))
        self.assertTrue(b.isdisjoint(d))
        self.assertTrue(b.isdisjoint(e))

    def test_cmp(self):
        a = set(["wagwaan", "hot", "skull"])
        b = set(["nba", "hang", "time"])
        c = core.Set(a)
        d = core.Set(b)
        self.assertEquals(a > b, c > d)
        self.assertEquals(a < b, c < d)
        self.assertEquals(a > b, c > b)
        self.assertEquals(a < b, c < b)
        self.assertEquals(a >= b, c >= d)
        self.assertEquals(a <= b, c <= d)
        self.assertEquals(a >= b, c >= b)
        self.assertEquals(a <= b, c <= b)
        self.assertEquals(a.issubset(b), c.issubset(d))
        self.assertEquals(a.issuperset(b), c.issuperset(d))
        self.assertEquals(a.issubset(b), c.issubset(b))
        self.assertEquals(a.issuperset(b), c.issuperset(b))


class DictTests(BaseTestCase):
    def test_value(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        self.assertEquals(core.Dict(a), a)

    def test_empty(self):
        self.assertEquals(core.Dict(), {})

    def test_update(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        b = {"wagwaan": "hotskull", "nba": "hangtime"}
        c = core.Dict(a)
        a.update(b)
        c.update(b)
        self.assertEquals(a, c)

    def test_iter(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        self.assertItemsEqual(iter(a), iter(core.Dict(a)))

    def test_keys(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        self.assertItemsEqual(a.keys(), core.Dict(a).keys())

    def test_values(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        self.assertItemsEqual(a.values(), core.Dict(a).values())

    def test_items(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        self.assertItemsEqual(a.items(), core.Dict(a).items())

    def test_setdefault(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        b = core.Dict(a)
        c = "nba"
        d = "hangtime"
        e = b.setdefault(c, d)
        self.assertEquals(e, d)
        self.assertEquals(b[c], d)
        self.assertEquals(a.setdefault(c, d), e)
        e = b.setdefault(c, c)
        self.assertEquals(e, d)
        self.assertEquals(a.setdefault(c, c), e)

    def test_get(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        b = core.Dict(a)
        self.assertEquals(a["wagwaan"], b["wagwaan"])
        self.assertEquals(a.get("wagwaan"), b.get("wagwaan"))
        self.assertRaises(KeyError, lambda: b["hotskull"])
        self.assertEquals(a.get("hotskull"), b.get("hotskull"))
        self.assertEquals(a.get("hotskull", "don"), b.get("hotskull", "don"))
        self.assertNotEquals(a.get("hotskull", "don"), b.get("hotskull", "x"))

    def test_set(self):
        a = core.Dict({"wagwaan": "popcaan", "flute": "don"})
        a["wagwaan"] = "hotskull"
        self.assertEquals(a["wagwaan"], "hotskull")

    def test_del(self):
        a = core.Dict({"wagwaan": "popcaan", "flute": "don"})
        del a["wagwaan"]
        self.assertRaises(KeyError, lambda: a["wagwaan"])

        def del_missing():
            del a["hotskull"]

        self.assertRaises(KeyError, del_missing)

    def test_len(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        b = core.Dict(a)
        self.assertEquals(len(a), len(b))

    def test_contains(self):
        a = {"wagwaan": "popcaan", "flute": "don"}
        b = core.Dict(a)
        self.assertIn("wagwaan", a)
        self.assertNotIn("hotskull", a)

    def test_copy(self):
        a = core.Dict({"wagwaan": "popcaan", "flute": "don"})
        b = a.copy()
        self.assertEquals(type(a), type(b))
        self.assertNotEquals(a.key, b.key)

    def test_clear(self):
        a = core.Dict({"wagwaan": "popcaan", "flute": "don"})
        a.clear()
        self.assertEquals(len(a), 0)

    def test_fromkeys(self):
        a = ["wagwaan", "hot", "skull"]
        b = "popcaan"
        c = core.Dict.fromkeys(a)
        self.assertItemsEqual(a, c.keys())
        ccc = c
        self.assertFalse(c["wagwaan"])
        c = core.Dict.fromkeys(a, b)
        self.assertEquals(c["wagwaan"], b)

    def test_defaultdict(self):
        a = "wagwaan"
        b = "popcaan"
        c = core.DefaultDict(lambda: b)
        self.assertEquals(c[a], b)
        c[b] += a
        self.assertEquals(c[b], b + a)


class StringTests(BaseTestCase):
    def test_value(self):
        a = "wagwaan"
        self.assertEquals(core.String(a), a)

    def test_empty(self):
        self.assertEquals(core.String(), "")

    def test_add(self):
        a = "wagwaan"
        b = "hotskull"
        c = core.String(a)
        d = core.String(b)
        self.assertEquals(a + b, core.String(a) + core.String(b))
        self.assertEquals(a + b, core.String(a) + b)
        d += c
        c += b
        self.assertEquals(a + b, c)
        self.assertEquals(b + a, d)

    def test_mul(self):
        a = "wagwaan"
        b = core.String(a)
        i = 9000
        self.assertEquals(a * i, core.String(a) * i)
        b *= i
        self.assertEquals(a * i, b)

    def test_len(self):
        a = "wagwaan"
        self.assertEquals(len(a), len(core.String(a)))

    def test_set(self):
        a = "wagwaan hotskull"
        b = "flute don"
        for i in range(0, len(b)):
            for j in range(i, len(b)):
                c = list(a)
                d = core.String(a)
                c[i:j] = list(b)
                d[i:j] = b
                c = "".join(c)
                self.assertEquals(d, c)

    def test_get(self):
        a = "wagwaan hotskull"
        b = core.String(a)
        self.assertEquals(a[4], b[4])
        self.assertEquals(a[3:12], b[3:12])
        self.assertEquals(a[:-5], b[:-5])
        self.assertRaises(IndexError, lambda: b[len(b)])

    def test_mutability(self):
        a = "wagwaan hotskull"
        b = "flute don"
        c = core.String(a)
        d = core.ImmutableString(a)
        keyC = c.key
        keyD = d.key
        a += b
        c += b
        d += b
        self.assertEquals(a, c)
        self.assertEquals(a, d)
        self.assertEquals(c.key, keyC)
        self.assertNotEquals(d.key, keyD)
        keyD = d.key
        i = 9000
        a *= i
        c *= i
        d *= i
        self.assertEquals(a, c)
        self.assertEquals(a, d)
        self.assertEquals(c.key, keyC)
        self.assertNotEquals(d.key, keyD)

        def immutable_set():
            d[0] = b

        self.assertRaises(TypeError, immutable_set)


class IntTests(BaseTestCase):
    def test_value(self):
        a = 420
        self.assertEquals(core.Int(a), a)

    def test_empty(self):
        self.assertEquals(core.Int(), 0)

    def test_add(self):
        a = 420
        b = 9000
        self.assertEquals(a + b, core.Int(a) + core.Int(b))
        self.assertEquals(a + b, core.Int(a) + b)
        c = core.Int(a)
        d = core.Int(b)
        d += c
        c += b
        self.assertEquals(a + b, c)
        self.assertEquals(b + a, d)

    def test_mul(self):
        a = 420
        b = core.Int(a)
        i = 9000
        self.assertEquals(a * i, core.Int(a) * i)
        b *= i
        self.assertEquals(a * i, b)

    def test_sub(self):
        a = 420
        b = 9000
        self.assertEquals(a - b, core.Int(a) - core.Int(b))
        self.assertEquals(a - b, core.Int(a) - b)
        c = core.Int(a)
        d = core.Int(b)
        d -= c
        c -= b
        self.assertEquals(a - b, c)
        self.assertEquals(b - a, d)

    def test_div(self):
        a = 420
        b = 9000
        self.assertEquals(a / b, core.Int(a) / core.Int(b))
        self.assertEquals(a / b, core.Int(a) / b)
        c = core.Int(a)
        d = core.Int(b)
        d /= c
        c /= b
        self.assertEquals(a / b, c)
        self.assertEquals(b / a, d)

    def test_mod(self):
        a = 420
        b = 9000
        self.assertEquals(a % b, core.Int(a) % core.Int(b))
        self.assertEquals(a % b, core.Int(a) % b)
        c = core.Int(a)
        d = core.Int(b)
        d %= c
        c %= b
        self.assertEquals(a % b, c)
        self.assertEquals(b % a, d)

    def test_pow(self):
        a = 4
        b = 20
        self.assertEquals(a ** b, core.Int(a) ** core.Int(b))
        self.assertEquals(a ** b, core.Int(a) ** b)
        c = core.Int(a)
        d = core.Int(b)
        d **= c
        c **= b
        self.assertEquals(a ** b, c)
        self.assertEquals(b ** a, d)

    def test_or(self):
        a = 420
        b = 9000
        self.assertEquals(a | b, core.Int(a) | core.Int(b))
        self.assertEquals(a | b, core.Int(a) | b)
        c = core.Int(a)
        d = core.Int(b)
        d |= c
        c |= b
        self.assertEquals(a | b, c)
        self.assertEquals(b | a, d)

    def test_xor(self):
        a = 420
        b = 9000
        self.assertEquals(a ^ b, core.Int(a) ^ core.Int(b))
        self.assertEquals(a ^ b, core.Int(a) ^ b)
        c = core.Int(a)
        d = core.Int(b)
        d ^= c
        c ^= b
        self.assertEquals(a ^ b, c)
        self.assertEquals(b ^ a, d)

    def test_lshift(self):
        a = 4
        b = 20
        self.assertEquals(a << b, core.Int(a) << core.Int(b))
        self.assertEquals(a << b, core.Int(a) << b)
        c = core.Int(a)
        d = core.Int(b)
        d <<= c
        c <<= b
        self.assertEquals(a << b, c)
        self.assertEquals(b << a, d)

    def test_rshift(self):
        a = 9000
        b = 4
        self.assertEquals(a >> b, core.Int(a) >> core.Int(b))
        self.assertEquals(a >> b, core.Int(a) >> b)
        c = core.Int(a)
        d = core.Int(b)
        d >>= c
        c >>= b
        self.assertEquals(a >> b, c)
        self.assertEquals(b >> a, d)


class FloatTests(BaseTestCase):
    def test_value(self):
        a = 420.666
        self.assertAlmostEqual(core.Float(a), a)

    def test_empty(self):
        self.assertEquals(core.Int(), .0)

    def test_add(self):
        a = 420.666
        b = 9000.666
        self.assertAlmostEqual(a + b, core.Float(a) + core.Float(b))
        self.assertAlmostEqual(a + b, core.Float(a) + b)
        c = core.Float(a)
        d = core.Float(b)
        d += c
        c += b
        self.assertAlmostEqual(a + b, c)
        self.assertAlmostEqual(b + a, d)

    def test_mul(self):
        a = 420.666
        b = core.Float(a)
        i = 9000.666
        self.assertAlmostEqual(a * i, core.Float(a) * i)
        b *= i
        self.assertAlmostEqual(a * i, b)

    def test_sub(self):
        a = 420.666
        b = 9000.666
        self.assertAlmostEqual(a - b, core.Float(a) - core.Float(b))
        self.assertAlmostEqual(a - b, core.Float(a) - b)
        c = core.Float(a)
        d = core.Float(b)
        d -= c
        c -= b
        self.assertAlmostEqual(a - b, c)
        self.assertAlmostEqual(b - a, d)

    def test_div(self):
        a = 420.666
        b = 9000.666
        self.assertAlmostEqual(a / b, core.Float(a) / core.Float(b))
        self.assertAlmostEqual(a / b, core.Float(a) / b)
        c = core.Float(a)
        d = core.Float(b)
        d /= c
        c /= b
        self.assertAlmostEqual(a / b, c)
        self.assertAlmostEqual(b / a, d)

    def test_mod(self):
        a = 420.666
        b = 9000.666
        self.assertAlmostEqual(a % b, core.Float(a) % core.Float(b))
        self.assertAlmostEqual(a % b, core.Float(a) % b)
        c = core.Float(a)
        d = core.Float(b)
        d %= c
        c %= b
        self.assertAlmostEqual(a % b, c)
        self.assertAlmostEqual(b % a, d)

    def test_pow(self):
        a = 4.666
        b = 20.666
        c = 4
        d = 2
        self.assertAlmostEqual(a ** b,
                               core.Float(a) ** core.Float(b))
        self.assertAlmostEqual(a ** b, core.Float(a) ** b)
        e = core.Float(a)
        f = core.Float(b)
        f **= c
        e **= d
        self.assertAlmostEqual(a ** d, e)
        self.assertAlmostEqual(b ** c, f)


class QueueTests(BaseTestCase):
    def test_put(self):
        a = "wagwaan"
        b = "hotskull"
        q = core.Queue(maxsize=2)
        q.put(a)
        self.assertIn(a, q)
        q.put(b)
        self.assertIn(b, q)
        self.assertRaises(Queue.Full, lambda: q.put("popcaan", block=False))
        start = time.time()
        timeout = 2
        try:
            q.put("popcaan", timeout=timeout)
        except Queue.Full:
            pass
        self.assertTrue(time.time() - start >= timeout)

    def test_get(self):
        a = "wagwaan"
        b = "hotskull"
        q = core.Queue()
        q.put(a)
        q.put(b)
        self.assertEquals(a, q.get())
        self.assertNotIn(a, q)
        self.assertEquals(b, q.get())
        self.assertNotIn(b, q)
        self.assertRaises(Queue.Empty, lambda: q.get(block=False))
        start = time.time()
        timeout = 2
        try:
            q.get(timeout=timeout)
        except Queue.Empty:
            pass
        self.assertTrue(time.time() - start >= timeout)

    def test_empty(self):
        q = core.Queue()
        self.assertTrue(q.empty())
        q.put("wagwaan")
        self.assertFalse(q.empty())
        q.get()
        self.assertTrue(q.empty())

    def test_full(self):
        q = core.Queue(maxsize=2)
        self.assertFalse(q.full())
        q.put("wagwaan")
        self.assertFalse(q.full())
        q.put("hotskull")
        self.assertTrue(q.full())
        q.get()
        self.assertFalse(q.full())

    def test_size(self):
        q = core.Queue()
        self.assertEquals(q.qsize(), 0)
        q.put("wagwaan")
        self.assertEquals(q.qsize(), 1)
        q.put("hotskull")
        self.assertEquals(q.qsize(), 2)
        q.get()
        self.assertEquals(q.qsize(), 1)

    def test_lifo_queue(self):
        a = "wagwaan"
        b = "hotskull"
        q = core.LifoQueue()
        q.put(a)
        q.put(b)
        self.assertEquals(b, q.get())
        self.assertNotIn(b, q)
        self.assertEquals(a, q.get())
        self.assertNotIn(a, q)

    def test_set_queue(self):
        a = "wagwaan"
        q = core.SetQueue()
        q.put(a)
        self.assertEquals(q.qsize(), 1)
        q.put(a)
        self.assertEquals(q.qsize(), 1)
        self.assertEquals(q.get(), a)
        self.assertEquals(q.qsize(), 0)


class CounterTest(object):
    def test_value(self):
        a = "wagwaan"
        b = {"hot": 420, "skull": -9000}
        c = collections.Counter(a)
        d = core.MultiSet(a)
        e = collections.Counter(**b)
        f = core.MultiSet(**b)
        self.assertEquals(d, c)
        self.assertEquals(f, e)

    def test_empty(self):
        self.assertEquals(core.MultiSet(), collections.Counter())

    def test_values(self):
        a = "wagwaan"
        b = {"hot": 420, "skull": -9000}
        c = collections.Counter(a)
        d = core.MultiSet(a)
        e = collections.Counter(**b)
        f = core.MultiSet(**b)
        self.assertItemsEqual(c.values(), d.values())
        self.assertItemsEqual(e.values(), f.values())

    def test_get(self):
        a = "wagwaan"
        b = {"hot": 420, "skull": -9000}
        c = collections.Counter(a)
        d = core.MultiSet(a)
        e = collections.Counter(**b)
        f = core.MultiSet(**b)
        self.assertEquals(c.get("a"), d.get("a"))
        self.assertEquals(c.get("flute", "don"), d.get("flute", "don"))
        self.assertEquals(e.get("hot"), f.get("hot"))
        self.assertEquals(e.get("skull"), f.get("skull"))
        self.assertEquals(e.get("flute", "don"), e.get("flute", "don"))

    def test_del(self):
        a = core.MultiSet("wagwaan")
        del a["hotskull"]

    def test_update(self):
        a = "wagwaan"
        b = {"hotskull": 420}
        b1 = [('1', 1), ('2', 2), ('3', 3)]
        c = collections.Counter(a)
        d = core.MultiSet(a)
        c.update(core.MultiSet(a))
        d.update(core.MultiSet(a))
        self.assertEqual(d, c)
        c = collections.Counter(a)
        d = core.MultiSet(a)
        c.update(collections.Counter(a))
        d.update(collections.Counter(a))
        self.assertEqual(d, c)
        c = collections.Counter(a)
        d = core.MultiSet(a)
        c.update(a)
        d.update(a)
        self.assertEqual(d, c)
        c = collections.Counter(a)
        d = core.MultiSet(a)
        c.update(b)
        d.update(b)
        self.assertEqual(d, c)
        c = collections.Counter(a)
        d = core.MultiSet(a)
        c.update(**b)
        d.update(**b)
        self.assertEqual(d, c)

        c.update(b1)
        d.update(b1)
        self.assertEqual(d, c)

    def test_subtract(self):
        a = "wagwaan"
        b = {"hotskull": 420}
        c = collections.Counter(a)
        d = core.MultiSet(a)
        c.subtract(core.MultiSet(a))
        d.subtract(core.MultiSet(a))
        self.assertEqual(d, c)
        c = collections.Counter(a)
        d = core.MultiSet(a)
        c.subtract(collections.Counter(a))
        d.subtract(collections.Counter(a))
        self.assertEqual(d, c)
        c = collections.Counter(a)
        d = core.MultiSet(a)
        c.subtract(a)
        d.subtract(a)
        self.assertEqual(d, c)
        c = collections.Counter(a)
        d = core.MultiSet(a)
        c.subtract(b)
        d.subtract(b)
        self.assertEqual(d, c)
        c = collections.Counter(a)
        d = core.MultiSet(a)
        c.subtract(**b)
        d.subtract(**b)
        self.assertEqual(d, c)

    def test_intersection(self):
        a = "wagwaan"
        b = "flute don"
        c = collections.Counter(a)
        d = core.MultiSet(a)
        c &= core.MultiSet(b)
        d &= core.MultiSet(b)
        self.assertEqual(d, c)
        c = collections.Counter(a)
        d = core.MultiSet(a)
        c &= collections.Counter(b)
        d &= collections.Counter(b)
        self.assertEqual(d, c)

    def test_union(self):
        a = "wagwaan"
        b = "flute don"
        c = collections.Counter(a)
        d = core.MultiSet(a)
        c |= core.MultiSet(b)
        d |= core.MultiSet(b)
        self.assertEqual(d, c)
        c = collections.Counter(a)
        d = core.MultiSet(a)
        c |= collections.Counter(b)
        d |= collections.Counter(b)
        self.assertEqual(d, c)

    def test_elements(self):
        a = "wagwaan"
        b = {"hotskull": 420}
        c = collections.Counter(a)
        d = core.MultiSet(a)
        e = collections.Counter(**b)
        f = core.MultiSet(**b)
        self.assertItemsEqual(c.elements(), d.elements())
        self.assertItemsEqual(e.elements(), f.elements())

    def test_most_common(self):
        a = "wanwaa"
        b = collections.Counter(a)
        c = core.MultiSet(a)
        d = 420
        self.assertEqual(c.most_common(d), b.most_common(d))
        self.assertEqual(c.most_common(), b.most_common())


class TestObjectList(BaseTestCase):
    def setUp(self):
        super(TestObjectList, self).setUp()

        self.client = get_redis_connection()

        self.list = [{1: 2}, {1: 3}, {1: 1}, {1: -1}]

        self.object_list = ObjectList(
            redis_key='ObjectList',
            client=self.client
        )
        self.object_list.extend(self.list)

    def test_sort_by_key(self):
        self.object_list.sort_by_key(lambda x: x[1])
        sorted_list = sorted(self.list, key=lambda x: x[1])

        self.assertEquals(
            list(self.object_list), sorted_list)

    def test___setitem__(self):
        self.object_list[1] = {1: 15}
        self.list[1] = {1: 15}

        self.assertEquals(list(self.object_list), self.list)

    def test___getitem__(self):
        for i in xrange(0, len(self.list)):
            self.assertEquals(self.object_list[i], self.list[i])

    def test_append(self):
        self.object_list.append({1: 15})
        self.list.append({1: 15})

        self.assertEquals(list(self.object_list), self.list)

    def test_extend(self):
        extension = [{1: 15}, {1: 13}]
        self.object_list.extend(extension)
        self.list.extend(extension)

        self.assertEquals(list(self.object_list), self.list)

    def test_insert(self):
        self.object_list.insert(3, {1: 15})
        self.list.insert(3, {1: 15})

        self.assertEquals(list(self.object_list), self.list)

    def test_test_pop(self):
        x = self.object_list.pop(3)
        x2 = self.list.pop(3)

        self.assertEquals(list(self.object_list), self.list)
        self.assertEquals(x, x2)

        x = self.object_list.pop(-1)
        x2 = self.list.pop(-1)

        self.assertEquals(list(self.object_list), self.list)
        self.assertEquals(x, x2)
