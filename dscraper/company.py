import logging
import asyncio

from .fetcher import CIDFetcher
from .utils import CountLatch, CommentFlow, validate_id
from .exporter import FileExporter
from .exceptions import Scavenger, DscraperError, NoMoreItems

_logger = logging.getLogger(__name__)

AID = 'AID'
CID = 'CID'


class BaseCompany:
    """Controls the number and operation of workers under the same policy.

    Can be: distributor, scavenger
    """
    _num_workers = 0

    def __init__(self, worker_ctor, scavenger, *, loop):
        self.loop = loop
        self.scavenger = scavenger or Scavenger()
        self._running = self._closed = False
        self._intended_workers = 0
        self._ctor = worker_ctor
        self._workers = set()
        self._latch = CountLatch(loop=self.loop)
        # Export methods
        self.success = self.scavenger.success
        self.is_dead = self.scavenger.is_dead

    async def run(self):
        self._running = True
        self.hire(self._intended_workers)
        del self._intended_workers
        await self._latch.wait()
        return self.stat()

    def hire(self, num=1):
        if not self._running:
            self._intended_workers += num
            return
        for _ in range(num):
            worker = self._ctor()
            fut = asyncio.ensure_future(worker.run(), loop=self.loop)
            fut.add_done_callback(lambda x: self._on_fired(x.result()))
            self._workers.add(worker)
        self._latch.count(num)
        self.scavenger.set_recorders(len(self._workers))

    def fire(self, num=1):
        """Try to stop at most num running workers."""
        if num < 0:
            num = len(self._workers)
        else:
            num = min(len(self._workers), num)
        for worker in self._workers:
            if num <= 0:
                break
            if not worker.is_stopped():
                worker.stop()  # Ask a worker to stop after completing its current task
                num -= 1

    def _on_fired(self, worker):
        """The actual method to stop a worker."""
        _logger.debug('A worker is done')
        worker.stop()
        self._latch.count_down()
        self._workers.remove(worker)
        self.scavenger.set_recorders(len(self._workers))

    def failure(self, worker, e=None):
        self.scavenger.failure(worker, e)
        if self.scavenger.is_dead():
            _logger.critical('Company is down due to too many expections!')
            self.close()

    def close(self):
        self._closed = True
        self.fire(-1)

    def stat(self):
        raise NotImplementedError


class CidCompany(BaseCompany):
    """Taking charge of the CommentWorkers.
    """
    UPDATE_INTERVAL = 5 * 60

    def __init__(self, distributor, *, scavenger=None, exporter=None, history=True, start=None,
                 end=None, loop):
        ctor = lambda: CommentWorker(distributor=self, exporter=self.exporter, scavenger=self,
                                     history=history, start=start, end=end, loop=loop)
        super().__init__(ctor, scavenger, loop=loop)
        self.distributor = distributor
        self.post = distributor.post
        self.post_list = distributor.post_list
        self.set = distributor.set
        self.exporter = exporter or FileExporter(loop=loop)
        self._checkpoint = True

    async def claim(self):
        # Update status
        if self._checkpoint:
            done = self.scavenger.get_success_count()
            num_items = len(self.distributor)
            if num_items is None:
                _logger.info('Progress: %d', done)
            else:
                _logger.info('Progress: %d (%.1f%%)', done, done / num_items * 100)
            self._checkpoint = False
            self.loop.call_later(self.UPDATE_INTERVAL, self._enable_checkpoint)

        cid = await self.distributor.claim()
        validate_id(cid)
        if self._closed:
            raise NoMoreItems('call it a day')
        return cid

    def stat(self):
        stats = ['-----', 'CID Scraping']

        total = len(self.distributor)
        cnt_success = self.scavenger.get_success_count()
        failures = self.scavenger.get_failures()
        items_rem = list(self.distributor.dump(1001))
        cnt_items_rem = len(items_rem)

        if total is None:
            total = cnt_success + len(failures) + cnt_items_rem if cnt_items_rem < 1001 else 'unknown'
        stats.append('Total number of targets: {}'.format(total))

        stats.append('Number of targets scraped: {}'.format(cnt_success))

        if failures:
            stats.append('Exceptions occured at CID: {}'.format(', '.join(failures)))

        if cnt_items_rem > 0:
            srem = 'List of targets yet to be scraped at CID: {}'.format(', '.join(items_rem))
            srem += '... (1000+ items)' if cnt_items_rem >= 1001 else \
                ' ({} items in total)'.format(cnt_items_rem)
        else:
            srem = 'All targets are scraped successfully!' if cnt_success == total else \
                'All targets are either scraped successfully or triggered exceptions'
        stats.append(srem)

        return '\n'.join(stats)

    def _enable_checkpoint(self):
        self._checkpoint = True


class AidCompany(BaseCompany):
    """Taking charge of the AVWorkers.

    TODO hire and fire workers according to items in the queue
    TODO set CidCompany's distributor final when StopIteration
    """

    def __init__(self, *, loop):
        raise NotImplementedError

    async def claim(self):
        # TODO use a dict to store {cid: aid}, then see dump
        raise NotImplementedError

    async def dump(self):
        # TODO through the dict from claim, and export cid as well aid
        raise NotImplementedError


class BaseWorker:

    def __init__(self, *, exporter, distributor, scavenger, fetcher):
        self.exporter, self.distributor, self.scavenger, self.fetcher = \
            exporter, distributor, scavenger, fetcher
        self.item = None
        self._stopped = False

    async def run(self):
        async with self.fetcher:
            while not self._stopped:
                # TODO update progress, already scraped, current scraping <- in dtor.claim()?
                try:
                    item = self.item = await self.distributor.claim()
                    data = await self._next(item)
                    await self.exporter.dump(item, data)
                except NoMoreItems:
                    break
                except DscraperError as e:
                    self.scavenger.failure(self, e)
                except Exception:
                    self.scavenger.failure(self)
                else:
                    self.scavenger.success()
        return self

    def stop(self):
        """Mostly called by company upon finishing. Worker does not call it itself."""
        self._stopped = True

    def is_stopped(self):
        return self._stopped

    async def _next(self, item):
        """
        :return object: data to be exported
        """
        raise NotImplementedError


class CommentWorker(BaseWorker):
    """Scrape all comments by CID
    """
    # note: elements returned may not be sorted, for example /12.xml
    # TODO int is not enough for comment_id. use long instead!

    def __init__(self, *, distributor, scavenger, exporter, history, loop, start=None, end=None):
        super().__init__(distributor=distributor, scavenger=scavenger,
                         fetcher=CIDFetcher(loop=loop), exporter=exporter)
        self.start = start if start is not None else 0
        self.end = end if end is not None else CommentFlow.MAX_TIMESTAMP
        self.history = history
        self._time_range = start is not None or end is not None
        if self._time_range:
            _logger.debug('time range is set')

    async def _next(self, cid):
        """Make a minimum number of requests to scrape all comments including history.

        Deleted comments may exist on certain dates but not on others. This worker
        does not guarantee that deleted comments are included, considering that
        they are not supposed to appear anyway.

        Note: no equality can be used when comparing timestamps because of duplication
        Note: if comments are not sorted, the first comment in each file is not
            necessarily the earliest, but the timestamps in Roll Date are still valid
        """
        # root is always scraped, regardless of ending timestamp. For complete header?
        # Must be parsed as XML for formatting
        latest = await self.fetcher.get_comments_root(cid)

        # Check if there are history comments
        history = False
        limit = self._find_int(latest, 'maxlimit', 1)
        if self.history:
            segments = self._digest(latest)
            normal = segments[0]
            if len(normal) >= limit:  # no less comments than a file could contain
                first_date = normal[0].attrib['date']
                ds = self._find_int(latest, 'ds', 0)  # ds may not be provided
                start, end = max(self.start, ds), min(self.end, first_date)
                if start <= end:  # not all comments are in time range
                    history = True

        if history:
            pools = tuple([segment] for segment in segments)  # pool is a list of segments
            histories, roll_dates = await self._scrape_history(cid, pools, limit, start, end)
            flows = [self._join(reversed(pool)) for pool in pools]  # Join segments into flows
            if self._time_range:  # If time range is set, the comments are not splitted
                for flow in flows:
                    self._trim(flow, self.start, self.end)
                roll_dates = None
        else:
            histories = flows = roll_dates = None
        return CommentFlow(latest, histories, flows, roll_dates, limit)

    async def _scrape_history(self, cid, pools, limit, start, end):
        """
        :return histories, roll_dates:
        """
        _logger.debug('scraping cid: %d', cid)
        # Scrape the history, and append each comment into its pool (normal/protected)
        roll_dates = await self.fetcher.get_rolldate_json(cid)
        _logger.debug('roll_dates: %s', roll_dates)
        histories = {}
        for idate in range(len(roll_dates) - 1, -1, -1):
            if idate != 0:
                if roll_dates[idate - 1] > end:
                    continue
                elif roll_dates[idate] < start:  # if idate == 0, assert roll_dates[idate] >= start
                    break

            date = roll_dates[idate]
            _logger.debug('scraping timestamp: %s', date)
            root = await self.fetcher.get_comments_root(cid, date)
            segments = self._digest(root)
            for pool, segment in zip(pools, segments):
                pool.append(segment)
            histories[date] = root

            normal = segments[0]
            if len(normal) < limit:
                break
            end = normal[0].attrib['date']  # assert len(normal) > 0 and date < end
            if start > end:
                break

        return histories, roll_dates

    @staticmethod
    def _find_int(root, tag, default):
        element = root.find(tag)
        return int(element.text) if element is not None else default

    @staticmethod
    def _digest(root):
        """
        :return ([normal_comments], [protected_comments], [title_comments], [code_comments]):
        """
        # Note: structure of comment document:
        #       +--------------------+
        #       | Header             |
        #       +--------------------+
        #       | Normal comments    |
        #       | ...                |
        #       | ...                |
        #       | ...                |
        #       | ...                |
        #       +--------------------+
        #       | Protected comments |
        #       | ...                |
        #       +--------------------+
        #       | Title comments     |
        #       | ...                |
        #       +--------------------+
        #       | Code comments      |
        #       | ...                |
        #       +--------------------+
        #
        # In each segment, comments are supposed to be sorted by their IDs in an
        # increasing order. Therefore, the ID of the last comment in a segment
        # is larger than that of the next comment, which is the first one in the
        # next segment. If we find these two comments, we also find the boundary
        # between two segments.
        # Unlike title and code comments, protected comments are not explicitly
        # declared in the XML. The only way to distinguish protected comments
        # from normal ones is to find the boundary between them.
        #
        # Note: if comments in any segment are not sorted, the output is undefined

        # Add indentation to the last element
        cmts = root.findall('d')
        ifront = length = len(cmts)

        irear = ifront
        for i in range(irear - 1, -1, -1):
            if cmts[i].attrib['pool'] != 2:
                ifront = i + 1
                break
        code = cmts[ifront:irear]

        irear = ifront
        for i in range(irear - 1, -1, -1):
            if cmts[i].attrib['pool'] != 1:
                ifront = i + 1
                break
        title = cmts[ifront:irear]

        irear = ifront
        last_id = CommentFlow.MAX_CMT_ID
        for i in range(irear - 1, -1, -1):
            cmt_id = cmts[i].attrib['id']
            if cmt_id > last_id:  # boundary found
                ifront = i + 1
                break
            last_id = cmt_id
        protected = cmts[ifront:irear]

        normal = cmts[:ifront] if ifront < length else cmts
        return normal, protected, title, code

    @staticmethod
    def _join(pool):
        """Join a list of mostly ascending segments."""
        flow = []
        horizon = 0
        for segment in pool:
            for i, cmt in enumerate(segment):
                if cmt.attrib['id'] > horizon:
                    horizon = segment[-1].attrib['id']
                    flow.extend(segment[i:])
                    break
        return flow

    @staticmethod
    def _trim(flow, start, end):
        """Discard all comments whose dates are not in the range [start, end]"""
        if start > end:
            return
        length = len(flow)
        ifront, irear = length, 0
        for i, cmt in enumerate(flow):
            if cmt.attrib['date'] >= start:
                ifront = i
                break
        for i, cmt in enumerate(reversed(flow)):
            if cmt.attrib['date'] <= end:
                irear = length - i
                break
        if not (ifront == 0 and irear == length):
            flow[:] = flow[ifront:irear]
