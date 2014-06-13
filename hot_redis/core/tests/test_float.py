from tests import BaseTestCase
from hot_redis.utils import make_key

from hot_redis import core, ObjectList

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
