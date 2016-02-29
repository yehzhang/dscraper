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
        self._time_range = start is None and end is None

    async def _next(self, cid):
        """Make a minimum number of requests to scrape all comments including history.

        Deleted comments may exist on certain dates but not on others. This worker
        does not guarantee that deleted comments are included, considering that
        they are not supposed to appear anyway.

        Note: no equality can be used when comparing timestamps because of duplication
        Note: if comments are not sorted, the first comment in each file is not
            necessarily the earliest, but the timestamps in Roll Date are still valid
        """
        # root is always scraped for headers, regardless of ending timestamp
        root = await self.fetcher.get_comments_root(cid)
        limit = self._find_int(root, 'maxlimit', 1)
        flow = CommentFlow(root, limit)
        if self.history:
            await self._history(cid, root, flow, limit)
        if self._time_range:
            flow.trim(self.start, self.end)
        return flow

    async def _history(self, cid, root, flow, limit):
        # Check if there are history comments
        segments = self._digest(root)
        normal = segments[0]
        if len(normal) < limit: # less comments than the file could contain
            return
        # Inspect both timestamp and count, because timestamp may not be provided
        first_date = normal[0].attrib['date']
        ds = self._find_int(root, 'ds', 0)
        start, end = max(self.start, ds), min(self.end, first_date)
        if start > end: # all comments are in time range already
            return

        # Scrape the history, and append each comment into its pool (normal/protected)
        roll_dates = await self.fetcher.get_rolldate_json(cid)
        # If time range is set, the comments are not splitted
        if not self._time_range:
            flow.set_splitter(roll_dates)
        flow.prepend(*segments)
        for idate in range(len(roll_dates) - 1, -1, -1):
            if idate != 0:
                if roll_dates[idate - 1]['timestamp'] > end:
                    continue
                elif roll_dates[idate]['timestamp'] < start:
                    break

            date = roll_dates[idate]['timestamp']
            root = await self.fetcher.get_comments_root(cid, date)
            segments = self._digest(root)
            if segments[1] or segments[2] or segments[3]:
                _logger.debug('Special comments found at cid %d, %d', cid, date)
            flow.prepend(*segments)

            if len(normal) < limit:
                break
            end = normal[0].attrib['date'] # assert len(normal) > 0 and date < end
            if start > end:
                break

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
        length = len(cmts)
        ifront = 0

        irear = ifront
        for i in range(irear - 1, -1, -1):
            if cmts[i].attrib['pool'] != 2:
                ifront = i + 1
                break
        code = cmts[ifront:]

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
