import Queue
import time
from tests import BaseTestCase

from hot_redis import core


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
