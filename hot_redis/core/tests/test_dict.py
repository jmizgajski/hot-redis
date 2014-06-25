from tests import BaseTestCase

from hot_redis import core


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
        self.assertIn("wagwaan", b)
        self.assertNotIn("hotskull", b)

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
