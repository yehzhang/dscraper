import logging
import asyncio
from collections import deque
from datetime import datetime
from pytz import timezone
from collections import defaultdict

from .fetcher import Fetcher
from .exporter import FileExporter
from .exceptions import (InvalidCid, DscraperError, Scavenger)
from .utils import parse_comments_xml, parse_rolldate_json, merge_xmls
from .company import CIDCompany, AIDCompany, CID, AID

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
    """The main class of dcraper.

    Controls the operation and communication among other modules.
    :TODO: add user interface during running using the curses library
    """
    MAX_WORKERS = 24

    def __init__(self, exporter=None, history=True, max_workers=6, *, loop=None): # TODO max_workers = 3?
        if not 0 < max_workers <= self.MAX_WORKERS:
            raise ValueError('number of workers is not in range [1, {}]'.format(self.MAX_WORKERS))
        self.loop = loop or asyncio.get_event_loop()
        self.exporter = exporter or FileExporter(loop=self.loop)
        self.history = history
        self.scavenger = Scavenger()
        self.workers = max_workers
        self._iters = defaultdict(list)

    def add(self, target, company_type=CID):
        """
        :param int target:
        """
        self.add_list([target], company_type)

    def add_range(self, start, end, company_type=CID):
        """Adds a range of targets, inclusive.
        :param int start:
        :param int end:
        """
        if start <= 0 or end < start:
            raise ValueError('not a valid range: [{} - {}]'.format(start, end))
        self.add_list(range(start, end + 1), company_type)

    def add_list(self, targets, company_type=CID):
        """
        :param iterable targets: an iterable of integers
        """
        try:
            target_iter = iter(targets)
        except TypeError as e:
            raise TypeError('target not iterable') from e
        self._iters[company_type].append(target_iter)

    def run(self):
        scavenger = Scavenger()
        distributor = None
        exporter = self.exporter
        workers = self.workers
        companies = []

        # TODO Build the AIDCompany
        # aid_targets = self._iters[AID]
        # if aid_targets:
        #     distributor = Distributor(self.loop)
        #     distributor.set()
        #     company = AIDCompany() # TODO
        #     distributor = company
        #     aid_workers = min(round(workers / 3), 1)
        #     workers -= aid_workers
        #     companies.append(company)
        #     for target in self._iters[AID]:
        #         company.post(target)

        # Build the CIDCompany
        if distributor is None:
            # If there is no AIDCompany upstream, set the default distributor
            distributor = Distributor(self.loop)
            distributor.set()
        company = CIDCompany(distributor, history=self.history,
                             scavenger=scavenger, exporter=exporter,
                             loop=self.loop, num_workers=workers)
        companies.append(company)
        for target in self._iters[CID]:
            company.post(target)

        self.loop.run_until_complete(self.exporter.connect())
        asyncio.ensure_future(self._patrol())
        results = self.loop.run_until_complete(asyncio.gather(*[com.run() for com in companies]))
        self.loop.run_until_complete(self.exporter.disconnect())

        # TODO sum up the results


    async def _patrol():
        # TODO read from the command line and update states. stop the scraper by calling distributor.close
        pass

class BaseWorker:

    def __init__(self, *, exporter, distributor, scavenger, fetcher):
        self.exporter = exporter
        self.distributor = distributor
        self.scavenger = scavenger
        self.fetcher = fetcher
        self._stopped = False

    async def run(self):
        async with self.fetcher:
            while not self._stopped and not self.scavenger.is_dead():
                # TODO update progress, already scraped, current scraping
                try:
                    first, second = await self.distributor.claim()
                    data = await self._next(first)
                    await self.exporter.dump(first, second, **data)
                except StopIteration:
                    break
                except DscraperError as e:
                    self.scavenger.failure(self, e)
                except:
                    self.scavenger.failure(self, None)
                else:
                    self.scavenger.success()

    def stop(self):
        self._stopped = True

    async def _next(item):
        raise NotImplementedError

class CommentWorker(BaseWorker):

    def __init__(self, *, distributor, scavenger, fetcher, exporter, history):
        super().__init__(distributor=distributor, scavenger=scavenger, fetcher=fetcher, exporter=exporter)
        self.history = history

    async def _next(self, item):
        # Get the data
        root = parse_comments_xml(await self.fetcher.get_comments(item))
        # Get the history data
        roll_date = None
        if self.history:
            # Continue only if the latest data contains comments no less than it could at maximum
            # note: the elements in XML are not always sorted by tags, for example /12.xml
            limit = root.find('maxlimit')
            try:
                limit = int(limit.text)
            except (TypeError, ValueError):
                pass
            else:
                num_comments = len(root.findall('d') or [])
                if num_comments >= limit:
                    roll_date = parse_rolldate_json(await self.fetcher.get_rolldate(self.cid))
                    await self._history(root, roll_date)
        return {'data': root,
                'splitter': roll_date}

    async def _history(self, root, roll_date):
        """Scrapes all history comments on the Roll Date using minimum requests, and appends
        the result to the root XML object in sorted order.
        Does not scrape all history files from the Roll Date unless some normal comments
        in one of the files are not sorted by their timestamps, which is the fundamental
        assumpution of the algorithm implemented here to reduce requests.

        :param XML root:
        :param JSON roll_date:
        """
        pass

class Distributor:
    """Distributes items from lists on demand. Support blocking when there is
    no items available.
    """
    def __init__(self, *, loop):
        self._queue = deque()
        self._iter = None
        self._latch = Sluice(loop=loop)
        self.set = self._latch.set
        self.is_set = self._latch.is_set

    def post(self, it):
        """
        :param iterator it:
        """
        if self.is_set():
            raise RuntimeError('distributor does not accept items anymore')
        # TODO priority post
        self._queue.append(it)
        self._latch.leak()

    async def claim(self):
        """Polls an item.

        When there is no item available, block until there is. If this distributor
        is set and there is no item, raise StopIteration instead.
        """
        while True:
            if not self._iter:
                if self._queue:
                    self._iter = self._queue.popleft()
                elif self.is_set():
                    raise StopIteration('no items available')
                else:
                    # Wait until there are new items or this distributor is closed
                    await self._latch.wait()
                    continue
            try:
                return next(self._iter)
            except StopIteration:
                self._iter = None

    def close(self):
        """Close this distributor."""
        self._queue.clear()
        self._iter = None
        self.set()
