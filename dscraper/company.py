import logging
import asyncio

from .fetcher import CIDFetcher
from .utils import CountLatch, CommentFlow
from .exporter import FileExporter
from .exceptions import Scavenger, DscraperError, NoMoreItems

_logger = logging.getLogger(__name__)

AID = 'AID'
CID = 'CID'

class BaseCompany:
    """Controls the number and operation of workers under the same policy.

    Can be: distributor, scavenger
    """
    def __init__(self, worker_ctor, *, scavenger=None, loop):
        self.loop = loop
        self.scavenger = scavenger or Scavenger()
        self._closed = False
        self._running = False
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

    def hire(self, num=1):
        if not self._running:
            self._intended_workers += num
            return
        for _ in range(num):
            worker = self._ctor()
            fut = asyncio.ensure_future(worker.run(), loop=self.loop)
            fut.add_done_callback(lambda x: self._latch.count_down())
            self._workers.add(worker)
        self._latch.count(num)
        self.scavenger.set_recorders(len(self))

    def fire(self, num=1):
        assert num <= len(self) # DEBUG
        num = min(len(self), num)
        for _ in range(num):
            worker, _ = self._workers.pop()
            worker.stop()
        self.scavenger.set_recorders(len(self))

    def failure(self, worker, e):
        self.scavenger.failure(worker, e)
        if self.scavenger.is_dead():
            _logger.critical('Company is down due to too many expections!')
            self.close()
            # TODO also collect items yet to be scraped

    def close(self):
        self._closed = True
        self.fire(len(self._workers))

    def __len__(self):
        return len(self._latch)

class CIDCompany(BaseCompany):
    """Taking charge of the CommentWorkers.
    """
    def __init__(self, distributor, *, scavenger=None, exporter=None, history=True, loop):
        ctor = lambda: CommentWorker(distributor=self, exporter=self.exporter,
                                     scavenger=self, history=history, loop=loop)
        super().__init__(ctor, scavenger=scavenger, loop=loop)
        self.distributor = distributor
        self.exporter = exporter or FileExporter(loop=loop)
        self.post = distributor.post
        self.set = distributor.set

    async def claim(self):
        cid = await self.distributor.claim()
        # Validate the item
        try:
            cid = int(cid)
        except TypeError:
            raise TypeError('invalid cid from input: an integer is required, not \'{}\''
                            .format(type(cid).__name__)) from None
        if cid <= 0:
            raise ValueError('invalid cid from input: a positive integer is required, not \'{}\''
                             .format(cid))
        if self._closed:
            raise NoMoreItems('call it a day')
        return cid

    def close(self):
        super().close()
        self.distributor.close()

class AIDCompany(BaseCompany):
    """Taking charge of the AVWorkers.

    TODO hire and fire workers according to items in the queue
    TODO set CIDCompany's distributor final when StopIteration
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
            while not self._stopped and not self.scavenger.is_dead():
                # TODO update progress, already scraped, current scraping
                try:
                    item = self.item = await self.distributor.claim()
                    data = await self._next(item)
                    await self.exporter.dump(item, data)
                except NoMoreItems:
                    self.stop()
                    break
                except DscraperError as e:
                    self.scavenger.failure(self, e)
                except:
                    self.scavenger.failure(self, None)
                else:
                    self.scavenger.success()
        _logger.info('A worker is done')

    def stop(self):
        self._stopped = True

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
        super().__init__(distributor=distributor, scavenger=scavenger, fetcher=CIDFetcher(loop=loop), exporter=exporter)
        self.start, self.end = start or 0, end or CommentFlow.MAX_TIMESTAMP
        self.history = history
        self._time_range = start is not None or end is not None

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
        limit = self._find_int(latest, 'maxlimit', 1)
        histories, pools, roll_dates = await self._scrape_history(cid, latest, limit)
        flows = None
        if histories:
            # Join segments into flows
            flows = [self._join(reversed(pool)) for pool in pools]
            # If time range is set, the comments are not splitted
            if self._time_range:
                for flow in flows:
                    self._trim(flow, self.start, self.end)
                roll_dates = None
        flow = CommentFlow(latest, histories, flows, roll_dates, limit)
        return flow

    async def _scrape_history(self, cid, latest, limit):
        """
        :return histories, (normal_pool, protected_pool, title_pool, code_pool), roll_dates:
            pool is a list of segments
        """
        if not self.history:
            return None, None, None
        # Check if there are history comments
        segments = self._digest(latest)
        pools = tuple([segment] for segment in segments)
        normal = segments[0]
        if len(normal) < limit: # less comments than the file could contain
            return None, None, None
        first_date = normal[0].attrib['date']
        ds = self._find_int(latest, 'ds', 0) # ds may not be provided
        start, end = max(self.start, ds), min(self.end, first_date)
        if start > end: # all comments are in time range already
            return None, None, None
        del normal, segments, first_date, ds

        _logger.debug('scraping cid: %d', cid)
        # Scrape the history, and append each comment into its pool (normal/protected)
        roll_dates = await self.fetcher.get_rolldate_json(cid)
        _logger.debug('roll_dates:\n%s', roll_dates)
        histories = {}
        for idate in range(len(roll_dates) - 1, -1, -1):
            if idate != 0:
                if roll_dates[idate - 1] > end:
                    continue
                elif roll_dates[idate] < start: # if idate == 0, assert roll_dates[idate] >= start
                    break

            date = roll_dates[idate]
            _logger.debug('scraping timestamp: %s', date)
            root = await self.fetcher.get_comments_root(cid, date)
            segments = self._digest(root)
            if segments[1] or segments[2] or segments[3]:
                _logger.debug('Special comments found at cid %d, %d', cid, date)
            for pool, segment in zip(pools, segments):
                pool.append(segment)
            histories[date] = root

            normal = segments[0]
            if len(normal) < limit:
                break
            end = normal[0].attrib['date'] # assert len(normal) > 0 and date < end
            if start > end:
                break

        return histories, pools, roll_dates

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
            if cmt_id > last_id: # boundary found
                ifront = i + 1
                break
            last_id = cmt_id
        protected = cmts[ifront:irear]

        normal = cmts[:ifront] if ifront < length else cmts
        return normal, protected, title, code

    @staticmethod
    def _join(segments):
        """Join a list of mostly ascending segments."""
        flow = []
        horizon = 0
        for segment in segments:
            for i, cmt in enumerate(segment):
                if cmt.attrib['id'] > horizon:
                    horizon = segment[-1].attrib['id']
                    flow.extend(segment[i:])
                    break
        return flow

    @staticmethod
    def _trim(flow, start, end):
        """Discard all comments not in the time range [start, end]
        Also join all segments into flows internally.
        """
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
        flow[:] = flow[ifront:irear]
