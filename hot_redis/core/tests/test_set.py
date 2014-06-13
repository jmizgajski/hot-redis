from tests import BaseTestCase
from hot_redis.utils import make_key

from hot_redis import core, ObjectList

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
