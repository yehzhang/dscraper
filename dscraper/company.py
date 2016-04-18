import logging
import asyncio
import time
import datetime
import concurrent

from .fetcher import CIDFetcher
from .utils import CountLatch, CommentFlow, validate_id, FrequencyController, find_elems
from .exporter import FileExporter
from .exceptions import Scavenger, DscraperError, NoMoreItems

_logger = logging.getLogger(__name__)

AID = 'AID'
CID = 'CID'


class BaseCompany:
    """Controls the number and operation of workers under the same policy.

    Can be: distributor
    """

    def __init__(self, max_workers, worker_ctor, scavenger, *, loop):
        self._intended_workers = max_workers
        self.max_workers = 0
        self.loop = loop
        self.scavenger = scavenger
        self._closed = False
        self._ctor = worker_ctor
        self._workers = {}
        self._latch = CountLatch(loop=self.loop)

    async def run(self):
        self._hire(self._intended_workers)
        await self._latch.wait()
        return self.stat()

    def _hire(self, num=1):
        for _ in range(num):
            worker = self._ctor()
            fut = self._workers[worker] = asyncio.ensure_future(worker.run(), loop=self.loop)
            _logger.debug('A worker is hired %s', worker)
            fut.add_done_callback(lambda x: self._on_fired(x.result()))
        self.max_workers += num
        self._latch.count(num)
        self.scavenger.set_recorders(len(self._workers))

    def _fire(self, num=1, force=True):
        """Stop at most num running workers. If do not force workers, they would
        run until finishing their current work.
        """
        if num < 0:
            num = len(self._workers)
        else:
            num = min(len(self._workers), num)
        for worker, fut in self._workers.items():
            if num <= 0:
                break
            if not worker.is_stopped():
                worker.stop()  # Ask a worker to stop after completing its current task
                if force:
                    fut.cancel()
                num -= 1

    def _on_fired(self, worker):
        """The actual method to stop a worker."""
        _logger.debug('A worker is done %s', worker)
        self._workers.pop(worker)
        self.max_workers -= 1
        self._latch.count_down()
        self.scavenger.set_recorders(len(self._workers))

    def close(self):
        """Force the company to close."""
        self._closed = True
        self._fire(-1)

    def stat(self):
        raise NotImplementedError


class CidCompany(BaseCompany):
    """Taking charge of the CommentWorkers.
    """
    UPDATE_INTERVAL = 1 * 60

    def __init__(self, max_workers, distributor, *, scavenger, exporter, history, time_range,
                 loop):
        ctor = lambda: CommentWorker(distributor=self, exporter=exporter, scavenger=scavenger,
                                     history=history, time_range=time_range, loop=loop)
        super().__init__(max_workers, ctor, scavenger, loop=loop)
        self.distributor = distributor
        self.post = distributor.post
        self.post_list = distributor.post_list
        self.set = distributor.set
        self.get_total = distributor.get_total
        self.exporter = FileExporter(loop=loop)
        self._checkpoint = True
        self._t_start = time.time()
        self._controller = FrequencyController()

    async def claim(self):
        # Update status for every a few minutes
        if self._checkpoint:
            self._update()
            self._checkpoint = False
            self.loop.call_later(self.UPDATE_INTERVAL, self._enable_checkpoint)

        # Wait for the controller
        await self._controller.wait()

        # Claim an item
        # TODO Is the item lost on cancelled?
        cid = await self.distributor.claim()
        try:
            validate_id(cid)
        except:
            self._controller.release()
            raise

        if self._closed:
            self.distributor.post([cid], True)
            raise NoMoreItems('call it a day')
        return cid

    def _update(self):
        # Log current status
        done = self.scavenger.get_success_count()
        num_items = self.distributor.get_total()
        elapsed = datetime.timedelta(seconds=round(time.time() - self._t_start))
        if num_items is None:
            _logger.info('Progress: %d finished (time elapsed: %s)', done, elapsed)
        else:
            _logger.info('Progress: %.1f%% (%d finished, time elapsed: %s)',
                         done / num_items * 100, done, elapsed)

        # Check host's status
        # TODO adjust workers in a more flexible way, according to 5 / interval
        len_worker = len(self._workers)
        if self._controller.is_busy():
            if len_worker > 3:
                _logger.info('Entering rush hour, cutting down workers')
                self._fire(len_worker - 3, False)
        else:
            if len_worker < self.max_workers:
                _logger.info('Leaving rush hour, hiring more workers')
                self._hire(self.max_workers - len_worker)

    def stat(self):
        stats = ['-----', 'CID Scraping']

        total = self.distributor.get_total()
        cnt_success = self.scavenger.get_success_count()
        failures = sorted(self.scavenger.get_failures())
        cnt_failures = len(failures)
        items_rem = sorted(self.distributor.dump(1001))
        cnt_items_rem = len(items_rem)  # TODO total - succ - fail if possible

        if total is None:
            total = cnt_success + cnt_failures + cnt_items_rem \
                if cnt_items_rem < 1001 else 'unknown'
        stats.append('Total number of targets: {}'.format(total))

        stats.append('Number of targets scraped: {}'.format(cnt_success))

        if failures:
            stats.append('Exceptions occured at: {} ({} in total)'.format(
                ', '.join(map(str, failures)), cnt_failures))

        if cnt_items_rem > 0:
            if cnt_items_rem >= 1001:
                srem = '... (1000+ items)'
                items_rem = items_rem[:100]
            else:
                srem = ' ({} in total)'.format(cnt_items_rem)
            srem = 'List of targets yet to be scraped: {}'.format(
                ', '.join(map(str, items_rem))) + srem
        else:
            srem = 'All targets are scraped successfully!' if cnt_success == total else \
                'All targets are either scraped successfully or skipped due to exceptions'
        stats.append(srem)

        return '\n'.join(stats)

    def _enable_checkpoint(self):
        self._checkpoint = True

    def close(self):
        super().close()
        self._controller.free()


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
        # TODO __repr__ self description of args
        self.exporter, self.distributor, self.scavenger, self.fetcher = \
            exporter, distributor, scavenger, fetcher
        self.item = None
        self._stopped = False

    async def run(self):
        async with self.fetcher:
            while not self._stopped and not self.scavenger.is_dead():
                try:
                    item = self.item = await self.distributor.claim()  # claim a target
                    data = await self._next(item)  # get the data
                    self.item = None
                    await self.exporter.dump(item, data)  # export it
                except NoMoreItems:
                    break
                except Exception as e:
                    self.scavenger.failure(self, e)
                else:
                    self.scavenger.success()

        self.stop()
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
    # note: elements returned may not be sorted or in bad format like /12.xml

    def __init__(self, *, distributor, scavenger, exporter, history, loop, time_range):
        super().__init__(distributor=distributor, scavenger=scavenger,
                         fetcher=CIDFetcher(loop=loop), exporter=exporter)
        self.history = history
        self.start, self.end = time_range
        if self.start is None or self.end is None:
            self.start, self.end = 0, CommentFlow.MAX_TIMESTAMP
            self._has_time_range = False
        else:
            self._has_time_range = True
            _logger.debug('time range is set: start: %s, end: %s', self.start, self.end)

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
        has_history = False
        limit = self._find_int(latest, 'maxlimit', 1)
        segments = self._digest(latest)
        if self.history:
            if self._len_cmt_pool_1(segments) >= limit:  # no less comments than a file could contain
                normal = segments[0]
                first_date = normal[0].attrib['date']
                ds = self._find_int(latest, 'ds', 0)  # ds may not be provided
                print(self.start, ds, self.end, first_date)
                start, end = max(self.start, ds), min(self.end, first_date)
                if start <= end:  # not all comments are in time range
                    has_history = True

        # Deal with history stuff
        if has_history:
            pools = tuple([segment] for segment in segments)  # pool is a list of segments
            histories, roll_dates = await self._scrape_history(cid, pools, limit, start, end)
            flows = [self._join(reversed(pool)) for pool in pools]  # Join segments into flows
        else:
            histories = flows = roll_dates = None

        # Trucate comments if time_range was set
        if self._has_time_range:
            if has_history:  # only trim flows
                for flow in flows:
                    self._trim(flow, self.start, self.end)
                roll_dates = None
            else:  # only trim latest
                latest = find_elems(latest, CommentFlow.ROOT_HEADERS)
                for segment in segments:
                    self._trim(segment, self.start, self.end)
                    latest.extend(segment)

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

            if self._len_cmt_pool_1(segments) < limit:
                break
            normal = segments[0]
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

    @staticmethod
    def _len_cmt_pool_1(segments):
        """Return the number of normal or protected comments, which are in the first comment pool
        :param (normal_comments, protected_comments, _, _) pools:
        """
        return sum(map(len, segments[:2]))
