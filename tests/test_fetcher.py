import unittest
import asyncio
import requests
import logging

import dscraper
from dscraper import fetcher
from dscraper.fetcher import HOST_CID

from .utils import Test, timer

logger = logging.getLogger(__name__)

def reference_get(uri):
    r = requests.get('http://comment.bilibili.com' + uri)
    return r.text

def references_gets(uris):
    return {uri: reference_get(uri) for uri in uris}

class TestFetcher(Test):

    targets = [
        '/1.xml',
        '/2.xml',
        '/3.xml',
        '/4.xml',
        # '/5.xml',
        # '/6.xml',
        # '/7.xml',
        # '/8.xml',
        # '/9.xml',
        # '/10.xml',
        # '/11.xml',
        # '/12.xml',
        # '/13.xml',
        # '/14.xml',
        # '/15.xml',
        # '/16.xml',
        # '/17.xml',
        # '/18.xml',
        # '/19.xml',
        # '/20.xml',
        # '/21.xml',
        # '/22.xml',
        # '/23.xml',
        # '/24.xml',
        # '/25.xml',
        # '/26.xml',
        # '/27.xml',
        # '/28.xml',
        # '/29.xml',
        '/30.xml'
    ]
    bad_targets = [
        '/0.xml',
        # '/-1.xml',
        # '/-2.xml',
        # '/-3.xml',
        '/-4.xml'
    ]
    host = HOST_CID
    expected = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # On 30 targets, the reference time is about 10 seconds
        cls.expected = timer(references_gets, 'reference time', cls.targets)

    def setUp(self):
        self.maxDiff = None
        self.fs = []
        for _ in range(3):
            f = fetcher.BaseFetcher(self.host, loop=self.loop)
            self.loop.run_until_complete(f.open())
            self.fs.append(f)
        self.f = self.fs[0]

    def tearDown(self):
        for f in self.fs:
            self.loop.run_until_complete(f.close())

    def compare(self, actual, uri):
        if self.expected:
            expected = self.expected[uri]
            self.assertEqual(expected, actual, 'My result is not what is expected on {}'.format(uri))
        else:
            print('{} >> {}'.format(uri, actual))

    def run_compare(self, fut, uri):
        actual = timer(self.loop.run_until_complete, self.id(), fut(uri))
        self.compare(actual, uri)

    def run_compare_all(self, futs):
        fut = asyncio.gather(*futs)
        results = timer(self.loop.run_until_complete, self.id(), fut)
        for result in results:
            for uri, actual in result:
                self.compare(actual, uri)

    def run_multiple_compare(self, mycoros, uris):
        item_per_coro = len(uris) // len(mycoros)

        async def get_all(coro):
            ret = []
            for _ in range(item_per_coro):
                uri = uris.pop()
                ret.append((uri, await coro(uri)))
            return ret

        futs = [get_all(coro) for coro in mycoros]
        self.run_compare_all(futs)

    # @unittest.skip('duplicated case')
    def test_get(self):
        uri = self.targets[0]
        self.run_compare(self.f.get, uri)

    # @unittest.skip('duplicated case')
    def test_multiple_blocking_get(self):
        for uri in self.targets:
            self.run_compare(self.f.get, uri)

    # @unittest.skip('duplicated case')
    def test_404(self):
        try:
            self.run_compare(self.f.get, self.bad_targets[1])
        except dscraper.PageNotFound as e:
            pass
        else:
            self.fail('Exception is not thrown on fetching a 404 page')

    # @unittest.skip('duplicated case')
    def test_multiple_async_get(self):
        gets = [f.get for f in self.fs]
        self.run_multiple_compare(gets, self.targets)

