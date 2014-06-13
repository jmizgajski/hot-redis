from tests import BaseTestCase
from hot_redis.utils import make_key

from hot_redis import core, ObjectList

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

