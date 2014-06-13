from tests import BaseTestCase
from hot_redis.utils import make_key

from hot_redis import core, ObjectList

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
