import collections
from tests import BaseTestCase

from hot_redis import core, ObjectList

class TestCounter(BaseTestCase):
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
