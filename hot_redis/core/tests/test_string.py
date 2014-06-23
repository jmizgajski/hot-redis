from tests import BaseTestCase

from hot_redis import core


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
