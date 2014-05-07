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

    def test_intersection(self):
        self.fail("To be implemented")

    def test_intersection_update(self):
        self.fail("To be implemented")

    def test_difference(self):
        self.fail("To be implemented")

    def test_difference_update(self):
        self.fail("To be implemented")

    def test_symmetric_difference(self):
        self.fail("To be implemented")

    def test_symmetric_difference_update(self):
        self.fail("To be implemented")

    def test_disjoint(self):
        self.fail("To be implemented")

    def test_cmp(self):
        self.fail("To be implemented")

    def test_bisect(self):
        self.fail("To be implemented")

    def test_slice(self):
        self.fail("To be implemented")
