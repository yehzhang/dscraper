import unittest
import asyncio
import logging
import sys
import requests
import time
import xmltodict as x2d
import re

from dscraper.fetcher import Fetcher
from dscraper.exceptions import DataError


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
        '/1.xml',
        '/2.xml',
        '/4.xml'
    ]
    expected = {uri: reference_fetch(uri) for uri in targets}

    def setUp(self):
        self.maxDiff = None
        self.mtime = self.rtime = None
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.f = Fetcher(loop=self.loop)
        self.loop.run_until_complete(self.f.open())

    def tearDown(self):
        self.f.close()
        self.loop.close()

    def compare(self, mfn, uri, rfn=None):
        fut = asyncio.ensure_future(mfn(uri))
        actual, time = timer(self.loop.run_until_complete, fut)

        if rfn is None:
            expected = self.expected[uri]
        else:
            expected = rfn(uri)

        self.assertEqual(expected, actual, 'unexpected result')
        return time

    def compare_s(self, mfn, uris, rfn=None):
        futs = (asyncio.ensure_future(mfn(uri)) for uri in uris)
        actuals, time = timer(self.loop.run_until_complete, asyncio.gather(*futs))

        for i, uri in enumerate(uris):
            if rfn is None:
                expected = self.expected[uri]
            else:
                expected = rfn(uri)
            self.assertEqual(expected, actuals[i], 'unexpected result')

        return time

    # @unittest.skip('duplicated field')
    def test_fetch(self):
        uri = self.targets[1]
        self.compare(self.f.fetch, uri)

    def test_multiple_auto_blocking_fetch(self):
        self.compare_s(self.f.fetch, self.targets)

    def test_multiple_manual_blocking_fetch(self):
        for uri in self.targets:
            self.compare(self.f.fetch, uri)

    # @unittest.skip('duplicated field')
    def test_get_xml(self):
        async def fetch_xml(uri):
            cid = re.match('/(\d+)', uri).group(1)
            return await self.f.fetch_comments(cid)

        self.compare_s(fetch_xml, self.targets, reference_fetch_xml)

