# -*- coding: utf-8 -*-
from blist import sortedset

from hot_redis.blist.types import SortedSet
from hot_redis.core.tests import BaseTestCase


class SortedSetTest(BaseTestCase):
    def test_value(self):
        a = sortedset({
            3,
            5,
            8
        })
        self.assertEquals(SortedSet(a), a)

    def test_empty(self):
        self.assertEquals(SortedSet(), sortedset())

    def test_add(self):
        a = sortedset({1, 2, 3})
        b = SortedSet(a)
        i = 4
        a.add(i)
        b.add(i)
        self.assertEquals(b, a)

    def test_update(self):
        a = sortedset({1, 2, 3})
        b = sortedset({5, 6, 9})
        c = sortedset({25, 26, 27})
        d = SortedSet(a)
        a.update(b, c)
        d.update(b, c)
        self.assertEquals(d, a)

    def test_pop(self):
        a = SortedSet([1, 2, 3])
        i = len(a)
        b = a.pop()
        self.assertEquals(len(a), i - 1)
        self.assertNotIn(b, a)

    def test_clear(self):
        a = SortedSet([1, 2, 3])
        a.clear()
        self.assertEquals(len(a), 0)

    def test_remove(self):
        a = SortedSet([1, 2, 3])
        i = len(a)
        b = 1
        a.remove(b)
        self.assertEquals(len(a), i - 1)
        self.assertNotIn(b, a)
        self.assertRaises(KeyError, lambda: a.remove(4))

    def test_discard(self):
        a = SortedSet([1, 2, 3])
        i = len(a)
        b = 1
        a.discard(b)
        self.assertEquals(len(a), i - 1)
        self.assertNotIn(b, a)
        self.assertEquals(a.discard(4), None)

    def test_len(self):
        a = sortedset({1, 2, 3})
        b = SortedSet(a)
        self.assertEquals(len(a), len(b))

    def test_contains(self):
        a = SortedSet([1, 2, 3])
        self.assertIn(1, a)
        self.assertNotIn(4, a)

    def test_slice(self):
        values = sortedset([
            ("kobyla", 7),
            ("ma", 6),
            ("maly", 5),
            ("bok", 4),
            ("jan", 3),
            ("maria", 2),
            ("rokita", 1),
        ], lambda x: x[1])
        subject = SortedSet(values, lambda x: x[1])

        calls = [
            (lambda i: i[:], '[:]'),
            (lambda i: i[3:4], '[3:4]'),
            (lambda i: i[-4:3], '[-4:3]'),
            (lambda i: i[2:], '[2:]'),
            (lambda i: i[:2], '[:2]'),
            (lambda i: i[2:1], '[2:1]'),
            (lambda i: i[-1:-2], '[-1:-2]')
        ]

        for call, code in calls:
            expected = call(values)
            result = call(subject)
            self.assertEquals(
                result,
                expected,
                "Results not equal when called with call={code}, "
                "got result={result}, expected={expected}".format(
                    code=code, result=result,
                    expected=expected
                )
            )

    def test_intersection_update(self):
        x = set([1,2,3,4])
        y = set([2,3,5])

        a = SortedSet(x)
        b = SortedSet(y)

        a.intersection_update(b)

        for e in x.intersection(y):
            self.assertIn(e, a)

        for e in x.difference(y):
            self.assertNotIn(e, a)

        for e in y:
            self.assertIn(e, b)

    def test_intersection(self):
        x = set([1,2,3,4])
        y = set([2,3,5])

        a = SortedSet(x)
        b = SortedSet(y)

        c = a.intersection(b)

        for e in x.intersection(y):
            self.assertIn(e, c)

        for e in x.difference(y):
            self.assertNotIn(e, c)

        for e in x:
            self.assertIn(e, a)

        for e in y:
            self.assertIn(e, b)

    def test_difference(self):
        x = set([1,2,3,4])
        y = set([2,3,5])

        a = SortedSet(x)
        b = SortedSet(y)

        c = a.difference(b)

        for e in x.difference(y):
            self.assertIn(e, c)

        for e in x.intersection(y):
            self.assertNotIn(e, c)

        for e in x:
            self.assertIn(e, a)

        for e in y:
            self.assertIn(e, b)

    def test_difference_update(self):
        a = SortedSet()
        self.assertRaises(NotImplementedError, a.difference_update, a)

    def test_symmetric_difference_update(self):
        a = SortedSet()
        self.assertRaises(NotImplementedError, a.symmetric_difference_update, a)

    def test_symmetric_difference(self):
        x = set([1,2,3])
        y = set([2,3,4])

        a = SortedSet(x)
        b = SortedSet(y)

        c = a.symmetric_difference(b)

        for e in x.symmetric_difference(y):
            self.assertIn(e, c)

        for e in x.intersection(y):
            self.assertNotIn(e, c)

        for e in x:
            self.assertIn(e, a)

        for e in y:
            self.assertIn(e, b)

    def test_disjoint(self):
        x = set([1,2,3,4])
        y = set([2,3,5])
        z = set([6,7,8])

        a = SortedSet(x)
        b = SortedSet(y)
        c = SortedSet(z)

        self.assertFalse(a.isdisjoint(b))
        self.assertTrue(a.isdisjoint(c))

    def test_cmp(self):
        x = set([1,2,3,4])
        y = set([2,3])
        z = set([3,4,8])

        a = SortedSet(x)
        b = SortedSet(y)
        c = SortedSet(z)

        self.assertTrue(b < a)
        self.assertTrue(a > b)

        self.assertFalse(b > a)
        self.assertFalse(a < b)

        self.assertFalse(a < c)
        self.assertFalse(c < a)

        self.assertFalse(a > c)
        self.assertFalse(c > a)
