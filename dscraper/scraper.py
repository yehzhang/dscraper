__all__ = ('Scraper', )

import logging
import asyncio
from collections import deque
from datetime import datetime
from pytz import timezone

from .fetcher import Fetcher
from .exporter import FileExporter
from .exceptions import (InvalidCid, DscraperError, Watcher)
from .utils import parse_xml, parse_json, merge_xmls, cid_filter, rec_cid_filter

_logger = logging.getLogger(__name__)

# _cooldown_duration = 1 # TODO adjust according to polite time, not belong here
# _fetcher = None

# async def get(cid, timestamp=0, loop=None):
#     global _fetcher
#     if not _fetcher or loop:
#         _fetcher = Fetcher(loop=loop)

#     async with _fetcher:
#         try:
#             return await _fetcher.fetch_xml(cid, timestamp)
#         except:
#             # TODO
#             raise

# async def scrape(mixed):
#     pass


class Scraper:

    def __init__(self, *, exporter=None, history=True, loop=None, workers=6): # TODO workers = 3?
        # Validate arguments
        if not loop:
            loop = asyncio.get_event_loop()
        if exporter is None:
            exporter = FileExporter(loop=loop)
        if workers <= 0:
            raise ValueError('not a valid number')

        watcher = Watcher()
        self.workers = [Worker(cid_iter=cid_iter, exporter=self.exporter,
                               watcher=watcher, loop=loop, history=history)
                        for _ in range(self.workers)]
        watcher.register(self.workers)

    def scrape_range(self, start, end):
        if start <= 0 or end < start:
            raise ValueError('not a valid range: [{} - {}]'.format(start, end))
        self._scrape(range(start, end + 1))

    def scrape_list(self, cids):
        """
        Parameter:
            cids: an iterable of integers
        """
        self._scrape(cid_filter(cids))

    def scrape_mixed(self, cids):
        """
        Parameter:
            cids: an iterable of integers and/or of iterables of integers
        """
        self._scrape(rec_cid_filter(cids))

    def _scrape(self, args):
        pass
        # start working
        # try:
            # TODO
            # # self.loop.run_until_complete(gather(workers.scrape_all()))
            # await gather
        # except :
            # raise

    # def _start(self):
    #     self.exporter.open()

    # def _end():
    #     self.exporter.close()
    #     pass


class Worker:

    def __init__(self, cid_iter, exporter, watcher, distributor, loop, history):
        self.targets = cid_iter
        self.exporter = exporter
        self.fetcher = Fetcher(loop=loop)
        self.watcher = watcher
        self.history = history
        self.distributor = distributor

    async def scrape_all(self):
        async with self.fetcher:
            while not self.watcher.is_dead():
                # TODO update progress, already scraped, current scraping
                try:
                    self.cid = await self.distributor.claim()
                    await self._scrape_next()
                except StopIteration:
                    break
                except DscraperError as e:
                    self.watcher.damage(e, self)
                except:
                    self.watcher.unexpected_damage(self)
                else:
                    self.watcher.heal()
                # TODO relax according to polite time

        # TODO sum up the result, report
        if self.watcher.is_dead():
            _logger.critical('Worker is down due to too many expections!')
        else:
            pass

    async def _scrape_next(self):
        # Get the data
        text = await self.fetcher.fetch_comments(self.cid)
        # Get the history data
        if self.history:
            root = parse_xml(text)
            # Continue only if the latest data contains comments no less than it could contain at maximum
            # Notice: the elements in XML are not always sorted by tags, for example /12.xml
            elimit = root.find('maxlimit')
            if elimit:
                limit = int(elimit.text)
                num_comments = len(root.findall('d') or [])
                if num_comments >= limit:
                    await self._scrape_history(root)
        # Export all data scraped in this round
        merge_xmls()
        await self.exporter.dump(self.cid, root, )

    async def _scrape_history(self, root):
        # TODO
        roll_dates = parse_json(await self.fetcher.fetch_rolldate(self.cid))
        pass

class Distributor:
    """Distribute items on demand. Support frequency control and time zone.

    :param tuple time_config: (tzinfo of the time zone where the host is,
                               duration of pause when scraping in rush hours,
                               start of the rush hours,
                               end of the rush hours)
    """

    def __init__(self, loop=None, time_config=BILIBILI_TIME_CONFIG):
        self.queue = deque()
        self.loop = loop or asyncio.get_event_loop()
        self.lock = asyncio.Lock(self.loop)
        self.tz, self.interval_busy, start, end = time_config
        if start > 23 or end < 0:
            raise ValueError('hour must be in [0, 23]')
        try:
            self.rush_hours = set(hour for hour in range(start, 24)) | set(hour for hour in range(0, end + 1))
        except TypeError as e:
            raise TypeError('hour must be integer') from e
        self.interval = 0

    def post(self, cid_iter):
        self.queue.append((cid for cid in cid_iter))

    def post_rec(self, cid_iters):
        # The depth of recursion is only 2 though
        def _rec_cid_iter():
            for cid_iter in cid_iters:
                try:
                    for cid in cid_iter:
                        yield cid
                except TypeError:
                    yield cid_iter
        self.post(_rec_cid_iter())

    def update_interval(self):
        hour_now = datetime.datetime.now(tz=self.tz).hour
        self.interval = self.interval_busy if hour_now in self.rush_hours else 0

    async def claim(self):
        # Poll an item anyway
        while True:
            if not self.iter:
                if self.queue:
                    self.iter = self.queue.popleft()
                else:
                    raise StopIteration('no items available')
            try:
                cid = next(self.iter)
            except StopIteration:
                self.iter = None
            else:
                break
        # Validate the item
        try:
            cid = int(cid)
        except TypeError:
            raise InvalidCid('Invalid cid from input: an integer is required, not \'{}\''.format(type(cid).__name__))
        if cid <= 0:
            raise InvalidCid('Invalid cid from input: a positive integer is required')
        # Frequency control
        self.update_interval()
        if self.interval > 0:
            await self.lock.acquire()
            self.loop.call_later(self.interval, self.lock.release)
        return cid

BILIBILI_TIME_CONFIG = (pytz.timezone('Asia/Shanghai'), 1, 7, 10)