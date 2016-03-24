import logging
import asyncio
import datetime
import time
from collections import deque, defaultdict
from itertools import chain, islice

from .exporter import FileExporter
from .exceptions import Scavenger, NoMoreItems
from .utils import Sluice, validate_id
from .company import CidCompany, AidCompany, CID, AID

_logger = logging.getLogger(__name__)

# _fetcher = None

# async def get(cid, timestamp=0, loop=None):
#     global _fetcher
#     if not _fetcher or loop:
#         _fetcher = Fetcher(loop=loop)

#     async with _fetcher:
#         try:
#             return await _fetcher.fetch_xml(cid, timestamp)
#         except Exception:
#             # TODO
#             raise

# async def scrape(mixed):
#     pass


class Scraper:
    """The main class of dcraper.

    Controls the operation and communication among other modules.
    TODO add user interface during running using the curses library
    TODO max_workers = 3? how to control maximum workers across companies? <- class variable
    """
    MAX_WORKERS = 24
    _IND = 'individual'

    def __init__(self, exporter=None, history=True, start=None, end=None, max_workers=6, *, loop=None):
        if not 0 < max_workers <= self.MAX_WORKERS:
            raise ValueError('number of workers is not in range [1, {}]'.format(self.MAX_WORKERS))
        self.loop = loop or asyncio.get_event_loop()
        self.exporter = exporter or FileExporter(loop=self.loop)
        self.history, self.start, self.end = history, start, end
        self.num_workers = max_workers  # Maybe unecessary since there is a class variable in class Company?
        self._iters = defaultdict(list)

    def add(self, target, company_type=CID):
        """
        :param int target: a positive integer representing a CID or AID
        """
        validate_id(target)
        self._iters[(company_type, self._IND)].append(target)
        return self

    def add_range(self, start, end, company_type=CID):
        """Adds a range of targets, inclusive.
        :param int start:
        :param int end:
        """
        if start <= 0 or end < start:
            raise ValueError('not a valid range: [{} - {}]'.format(start, end))
        self._iters[company_type].append(range(start, end + 1))
        return self

    def add_list(self, targets, company_type=CID):
        """
        :param iterable targets: an iterable of integers

        Note: cannot do type or value checking here because targets might be an
            iterator or infinite generator
        """
        try:
            _ = iter(targets)
        except TypeError:
            raise TypeError('target \'{}\' not iterable'.format(targets)) from None
        self._iters[company_type].append(targets)
        return self

    def run(self):
        start_time = time.time()
        stats = self._run()
        end_time = time.time()

        # Sum up the results
        stats.insert(0, 'Report')
        stats.append('-----')
        stats.append('Overall')
        stats.append('Finished in: {}'.format(
            datetime.timedelta(seconds=round(end_time - start_time))))
        stats.append('======\n')
        _logger.info('\n'.join(stats))

    def _run(self):
        scavenger = Scavenger()
        distributor = None
        exporter = self.exporter
        num_workers = self.num_workers
        companies = []

        # TODO Build the AidCompany
        # aid_targets = self._iters[AID]
        # if aid_targets:
        #     distributor = BlockingDistributor(self.loop)
        #     distributor.set()
        #     company = AidCompany() # TODO
        #     distributor = company
        #     aid_workers = min(round(num_workers / 3), 1)
        #     num_workers -= aid_workers
        #     companies.append(company)
        #     for target in self._iters[AID]:
        #         company.post(target)

        # Build the CidCompany
        if distributor is None:
            # If there is no AidCompany upstream, the CidCompany needs an initial distributor
            distributor = BlockingDistributor(loop=self.loop)
        company = CidCompany(distributor, history=self.history,
                             scavenger=scavenger, exporter=exporter,
                             loop=self.loop)
        company.hire(num_workers)
        targets = self._iters[CID]
        targets.append(self._iters[(CID, self._IND)])
        company.post_list(targets)
        company.set()
        companies.append(company)
        del self.history, self.start, self.end, scavenger, distributor, exporter, num_workers, \
            company, targets
        self._iters.clear()

        self.loop.run_until_complete(self.exporter.connect())
        asyncio.ensure_future(self._patrol())
        try:
            return self.loop.run_until_complete(asyncio.gather(*[com.run() for com in companies]))
        finally:
            self.loop.run_until_complete(self.exporter.disconnect())

    async def _patrol(self):
        # TODO read from the command line and update states. stop the scraper by
        # calling distributor.close
        pass


class BlockingDistributor:
    """Distributes items from iterables on demand. Block when there is
    no items available.
    """

    def __init__(self, *, loop):
        self._queue = deque()
        self._iter = None
        self._latch = Sluice(loop=loop)
        self.set = self._latch.set
        self.is_set = self._latch.is_set
        self._count = 0

    def post(self, it):
        """
        :param iterable it: can be a list or generator
        """
        if self.is_set():
            raise RuntimeError('distributor does not accept items anymore')
        if self._count is not None:
            try:
                self._count += len(it)
            except TypeError:
                self._count = None
        self._queue.append(iter(it))
        self._latch.leak()

    def post_list(self, its):
        """
        :param list its: a list of lists or generators
        """
        if self.is_set():
            raise RuntimeError('distributor does not accept items anymore')
        if self._count is not None:
            try:
                self._count += sum(map(len, its))
            except TypeError:
                self._count = None
        self._queue.extend(map(iter, its))
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
                    raise NoMoreItems('all items have been distributed')
                else:
                    # Wait until there are new items or this distributor is closed
                    await self._latch.wait()
                    continue
            try:
                return next(self._iter)
            except StopIteration:
                self._iter = None

    def dump(self, num=None):
        """Remove all items yet to be distributed, and return at most num of them."""
        iter_items = chain(self._iter or [], *self._queue)
        self.clear()
        if num is not None:
            iter_items = islice(iter_items, num)
        return iter_items

    def clear(self):
        """Remove all items yet to be distributed."""
        self._queue.clear()
        self._iter = None

    def __len__(self):
        return self._count
