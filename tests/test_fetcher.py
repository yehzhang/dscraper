import unittest
import asyncio
import logging
import sys
import requests
import time
import xmltodict as x2d
import re

import dscraper


logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def reference_fetch(uri):
    return requests.get('http://comment.bilibili.com' + uri).text

def reference_fetch_xml(uri):
    r = requests.get('http://comment.bilibili.com' + uri).text
    return x2d.parse(r)

def timer(fn, *args, **kwargs):
    start = time.time()
    result = fn(*args, **kwargs)
    end = time.time()
    elapsed = round(end - start, 3)
    return result, elapsed


class Test_Fetcher(unittest.TestCase):

    targets = [
        '/0.xml',
        '/1.xml',
        '/2.xml',
        '/3.xml',
        '/4.xml',
        '/5.xml',
        '/6.xml',
        '/7.xml',
        '/8.xml',
        '/9.xml'
    ]
    expected = {uri: reference_fetch(uri) for uri in targets[1:]}
    expected[targets[0]] = None

    @classmethod
    def setUpClass(cls):
        cls.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(cls.loop)

    @classmethod
    def tearDownClass(cls):
        cls.loop.close()

    def setUp(self):
        self.maxDiff = None
        self.f = dscraper.Fetcher(loop=self.loop)
        self.loop.run_until_complete(self.f.open())

    def tearDown(self):
        self.f.close()

    def compare(self, actual, uri, reffn=None):
        expected = self.expected[uri] if reffn is None else reffn(uri)
        self.assertEqual(expected, actual, 'My result is not what is expected on {}'.format(uri))

    def run_compare(self, myfn, uri, reffn=None):
        fut = asyncio.ensure_future(myfn(uri))
        actual = self.loop.run_until_complete(fut)
        self.compare(actual, uri, reffn)

    def run_compare_multiple(self, mycoro, uris, reffn=None):
        fut = asyncio.ensure_future(mycoro(uris))
        actuals = self.loop.run_until_complete(fut)
        for uri, actual in zip(uris, actuals):
            self.compare(actual, uri, reffn)

    def run_compare_all(self, myfn, uris, reffn=None):
        futs = (asyncio.ensure_future(myfn(uri)) for uri in uris)
        actuals = self.loop.run_until_complete(asyncio.gather(*futs))
        for uri, actual in zip(uris, actuals):
            self.compare(actual, uri, reffn)

    async def fetch_range(self, uris):
        actuals = []
        for uri in uris:
            actuals.append(await self.f.fetch(uri))
        return actuals

    # @unittest.skip('duplicated case')
    def test_fetch(self):
        uri = self.targets[1]
        self.run_compare(self.f.fetch, uri)

    # @unittest.skip('duplicated case')
    def test_multiple_auto_blocking_fetch(self):
        self.run_compare_multiple(self.fetch_range, self.targets[1:])

    # @unittest.skip('duplicated case')
    def test_multiple_manual_blocking_fetch(self):
        for i, uri in enumerate(self.targets[1:]):
            self.run_compare(self.f.fetch, uri)

    # @unittest.skip('duplicated case')
    def test_await_running_fetch_coroutine(self):
        try:
            self.run_compare_all(self.f.fetch, self.targets)
        except RuntimeError:
            logger.debug('RuntimeError raisen, correct')
        else:
            self.fail('RuntimeError should be raised on starting the already running fetch() coroutine')

    # @unittest.skip('duplicated case')
    def test_404(self):
        uri = self.targets[0]
        self.run_compare(self.f.fetch, uri)

    # TODO
    # def test_multipleerrors():
    #     pass
