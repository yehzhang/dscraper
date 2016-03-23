import unittest
import asyncio
import time


def timer(fn, message, *args, **kwargs):
    start = time.time()
    result = fn(*args, **kwargs)
    end = time.time()
    elapsed = round(end - start, 3)
    print('\n{}: {:.2f}'.format(message, elapsed))
    return result


class Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(cls.loop)

    @classmethod
    def tearDownClass(cls):
        cls.loop.close()

