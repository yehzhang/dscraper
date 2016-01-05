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

class Test_Fetcher(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None
        self.mtime = self.rtime = None
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.f = Fetcher(loop=self.loop)
        self.loop.run_until_complete(self.f.connect())
        self.targets = [
            '/1.xml',
            '/2.xml',
            '/4.xml'
        ]

    def tearDown(self):
        self.f.disconnect()
        tasks = asyncio.Task.all_tasks(self.loop)
        self.loop.close()
        if self.mtime and self.rtime:
            logger.debug('my time: %f, reference time: %f', self.mtime, self.rtime)

    def compare(self, mfn, rfn, uri):
        fut = asyncio.ensure_future(mfn(uri))
        actual, mtime = timer(self.loop.run_until_complete, fut)

        expected, rtime = timer(rfn, uri)

        self.assertEqual(expected, actual, 'unexpected result')
        return mtime, rtime

    def compare_s(self, mfn, rfn, uris):
        futs = (asyncio.ensure_future(mfn(uri)) for uri in uris)
        actuals, mttime = timer(self.loop.run_until_complete, asyncio.gather(*futs))

        rttime = 0
        for i, uri in enumerate(uris):
            expected, rtime = timer(rfn, uri)
            self.assertEqual(expected, actuals[i], 'unexpected result')
            rttime += rtime

        return mttime, rttime

    def test_fetch(self):
        uri = self.targets[1]
        self.compare(self.f.fetch, reference_fetch, uri)

    def test_multiple_auto_blocking_fetch(self):
        self.mtime, self.rtime = self.compare_s(self.f.fetch, reference_fetch, self.targets)

    def test_multiple_manual_blocking_fetch(self):
        mttime, rttime = 0, 0
        for uri in self.targets:
            mtime, rtime = self.compare(self.f.fetch, reference_fetch, uri)
            mttime += mtime
            rttime += rtime
        self.mtime, self.rtime = mttime, rttime

    def test_get_xml(self):
        async def fetch_xml(uri):
            cid = re.match('/(\d+)', uri).group(1)
            return await self.f.fetch_comments(cid)

        self.mtime, self.rtime = self.compare_s(fetch_xml, reference_fetch_xml, self.targets)

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