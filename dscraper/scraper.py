import logging
import asyncio
import datetime
import time
import io
from collections import deque, defaultdict
from itertools import chain, islice
import concurrent

from .exporter import FileExporter, StreamExporter
from .exceptions import Scavenger, NoMoreItems
from .utils import Sluice, validate_id, CommentFlow
from .company import CidCompany, AidCompany, CID, AID

_logger = logging.getLogger(__name__)

async def get(cid, history=True, *, loop=None):
    """Get the XML string of all comments."""
    if loop is None:
        loop = asyncio.get_event_loop()
    stream = io.StringIO()
    exporter = StreamExporter(stream, '', loop=loop)
    scraper = Scraper(exporter, history=history, max_workers=1, loop=loop)

    scraper.add(cid)
    await scraper.async_run()

    text = stream.getvalue()
    stream.close()
    return text

async def get_list(targets, history=True, *, loop=None):
    """Get a list of XML strings of the targets."""
    raise NotImplementedError

    end = chr(0)
    try:
        num_workers = min(len(targets), 6)
    except TypeError:
        num_workers = 6
    if loop is None:
        loop = asyncio.get_event_loop()
    stream = io.StringIO()
    exporter = StreamExporter(stream, end, loop=loop)
    scraper = Scraper(exporter, history=history, max_workers=num_workers, loop=loop)

    scraper.add_list(targets)
    await scraper.async_run()

    ltext = stream.getvalue().split(end)
    ltext.pop()
    stream.close()

    # TODO split result not sorted
    return ltext


class Scraper:
    """The main driver of dcraper. Controls the operation and communication among modules.

    :param exporter exporter: handler of data fetched
    :param bool history: whether scrape history comments or not
    :param (int/None, int/None) time_range: two unix timestamps specifying the starting and
        ending dates between which comments should be scraped (inclusive)
    :param int max_workers: maximum number of workers (connections) to one host the scraper
        could establish at the same time

    TODO add user interface during running using the curses library
    """
    MAX_WORKERS = 24
    _IND = 'individual'

    def __init__(self, exporter=None, history=True, time_range=None, max_workers=6, *,
                 loop=None):
        if not 0 < max_workers <= self.MAX_WORKERS:
            raise ValueError('number of workers is not in range [1, {}]'.format(self.MAX_WORKERS))
        if time_range is None:
            time_range = (None, None)
        else:
            start, end = time_range
            if start is None:
                start = 0
            if end is None:
                end = CommentFlow.MAX_TIMESTAMP
            try:
                start, end = map(int, (start, end))
            except TypeError:
                raise TypeError(
                    'Expected (\'int/None\', \'int/None\') as time range, not (\'{}\', \'{}\')'.format(
                        *map(type, time_range))) from None
            if start > end:
                raise ValueError('{} is not a valid time range'.format(time_range))
            time_range = (start, end)

        self.loop = loop or asyncio.get_event_loop()
        self.exporter = exporter or FileExporter(loop=self.loop)
        self.history, self.time_range = history, time_range
        self.max_workers = max_workers
        self._iters = defaultdict(list)
        self.companies = []

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
            raise ValueError('not a valid range: {} - {}'.format(start, end))
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
        """Run the scraper."""
        fut = asyncio.ensure_future(self.async_run())
        try:
            self.loop.run_until_complete(fut)
        except KeyboardInterrupt:
            for company in self.companies:
                company.close()
            self.loop.run_until_complete(fut)

    async def async_run(self):
        """The indeed main coroutine that can be awaited."""
        start_time = time.time()
        stats = await self._async_run()
        end_time = time.time()

        # Sum up the results
        stats.insert(0, 'Report')
        stats.append('-----')
        stats.append('Overall')
        stats.append('Finished in: {}'.format(
            datetime.timedelta(seconds=round(end_time - start_time))))
        stats.append('======\n')
        _logger.info('\n'.join(stats))

    async def _async_run(self):
        scavenger = Scavenger()
        distributor = None
        exporter = self.exporter
        self.companies.clear()

        # TODO Build the AidCompany
        # aid_targets = self._iters[AID]
        # if aid_targets:
        #     distributor = BlockingDistributor(self.loop)
        #     distributor.set()
        #     company = AidCompany() # TODO
        #     distributor = company
        #     self.companies.append(company)
        #     for target in self._iters[AID]:
        #         company.post(target)

        # Build the CidCompany
        if distributor is None:
            # If there is no AidCompany upstream, the CidCompany needs an initial distributor
            distributor = BlockingDistributor(loop=self.loop)
        # TODO max_workers = min(max_workers, len(disteibutor))
        company = CidCompany(self.max_workers, distributor, history=self.history,
                             scavenger=scavenger, exporter=exporter, time_range=self.time_range,
                             loop=self.loop)

        targets = self._iters[CID]
        targets.append(self._iters[(CID, self._IND)])
        company.post_list(targets)
        company.set()
        if company.get_total() == 0:
            _logger.info('No targets assigned')
            return []

        self.companies.append(company)
        del scavenger, distributor, exporter, company, targets
        self._iters.clear()

        await self.exporter.connect()
        asyncio.ensure_future(self._patrol())
        try:
            return await asyncio.gather(*[com.run() for com in self.companies])
        finally:
            await self.exporter.disconnect()

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

    def post(self, it, recycle=False):
        """
        :param iterable it: can be a list or generator
        """
        if self._count is not None and not recycle:
            try:
                self._count += len(it)
            except TypeError:
                self._count = None
        self._queue.append(iter(it))
        self._latch.leak()

    def post_list(self, its, recycle=False):
        """
        :param list its: a list of lists or generators
        """
        if self._count is not None and not recycle:
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
        if num is not None:
            iter_items = islice(iter_items, num)
        items = list(iter_items)
        self.clear()
        return items

    def clear(self):
        """Remove all items yet to be distributed."""
        self._queue.clear()
        self._iter = None

    def get_total(self):
        return self._count
