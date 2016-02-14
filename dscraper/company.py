import logging
import asyncio

from .fetcher import CIDFetcher
from .utils import CountLatch, parse_comments_xml, parse_rolldate_json, merge_xmls
from .exporter import FileExporter
from .exceptions import Scavenger, DscraperError

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
        self.__len__ = self._latch.__len__

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
        self.scavenger.set_recorders_by(num)

    def fire(self, num=1):
        assert num <= len(self) # DEBUG
        num = min(len(self), num)
        for _ in range(num):
            worker, _ = self._workers.pop()
            worker.stop()
        self.scavenger.set_recorders_by(-num)

    def failure(self, worker, e):
        self.scavenger.failure(worker, e)
        if self.scavenger.is_dead():
            _logger.critical('Company is down due to too many expections!')
            self.close()
            # TODO also collect items yet to be scraped

    def close(self):
        self._closed = True
        self.fire(len(self._workers))

class CIDCompany(BaseCompany):
    """Taking charge of the CommentWorkers.
    """
    def __init__(self, distributor, *, scavenger=None, exporter=None, history=True, loop):
        self.distributor = distributor
        self.exporter = exporter or FileExporter(loop=loop)
        ctor = lambda: CommentWorker(distributor=self, exporter=self.exporter,
                                     scavenger=self, history=history, loop=loop)
        super().__init__(ctor, scavenger=scavenger, loop=loop)
        self.post = self.distributor.post

    async def claim(self):
        cid = await self.distributor.claim()
        # Validate the item
        try:
            cid = int(cid)
        except TypeError as e:
            raise TypeError('invalid cid from input: an integer is required, not \'{}\''
                            .format(type(cid).__name__)) from e
        if cid <= 0:
            raise ValueError('invalid cid from input: a positive integer is required, not \'{}\''
                             .format(cid))
        if self._closed:
            raise StopIteration('call it a day')
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
                    cid = await self.distributor.claim()
                    xml, splitter = await self._next(cid)
                    await self.exporter.dump(cid=cid, xml=xml, splitter=splitter)
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

    async def _next(self, item):
        raise NotImplementedError

class CommentWorker(BaseWorker):

    def __init__(self, *, distributor, scavenger, exporter, history, loop, start=None, end=None):
        # TODO set latest or earliest comment timestamp
        super().__init__(distributor=distributor, scavenger=scavenger, fetcher=CIDFetcher(loop=loop), exporter=exporter)
        self.history = history
        self.start = start or 0
        self.end = end or 2147483647

    async def _next(self, cid):
        # Get the data
        root = parse_comments_xml(await self.fetcher.get_comments(cid))
        # Get the history data
        roll_dates = None
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
                    roll_dates = parse_rolldate_json(await self.fetcher.get_rolldate(cid))
                    root = await self._history(root, roll_dates)
        return (root, roll_dates)

    async def _history(self, root, roll_dates):
        """Scrapes all history comments on the Roll Date using minimum requests, and returns
        the result in sorted order as one XML object.
        Does not scrape all history files from the Roll Date unless some normal comments
        in one of the files are not sorted by their timestamps, which is the fundamental
        assumpution of the algorithm implemented here to reduce requests.

        :param XML root:
        :param JSON roll_dates:
        :return XML:
        """
        xmls = []
        latest_time = root # TODO
        start, end = self.start, min(self.end, latest_time - 1)
        # for($numRollDate = count($rollDates) - 1; $numRollDate >= 0; $numRollDate--)

        # loop through all rollDates to get comments and to insert them into database
        idate = len(roll_dates) - 1
        while idate >= 0:
            pass
            # look for the desirable the where the breakpoint locates or the first date
            # while idate > 0:
            # for inext_date in range(idate, 1, -1):
            #     next_time = roll_dates[inext_date - 1]['timestamp']
            #     if next_time <= headCommentTimestamp:
            #         break

            # if($numRollDate >= 1)
            # {
            #     $nextRollDateTimestamp = $rollDates[$numRollDate - 1]->timestamp;
            #     if($nextRollDateTimestamp > $headCommentTimestamp)
            #     {
            #         continue;
            #     }
            # }

            # # get uniserted comment entries of the date
            # $segmentOfHistoryCommentsXml = $this->getCommentsXml($rollDates[$numRollDate]->timestamp);
            # if(!($segmentOfHistoryCommentsXml instanceof DOMDocument))
            # {
            #     return $segmentOfHistoryCommentsXml;
            # }
            # $commentEntries = self::getSortedCommentsArray($segmentOfHistoryCommentsXml);
            # for($i = count($commentEntries) - 1; $i >= 0; $i--)
            # {
            #     if($commentEntries[$i][self::CMTENT_ATTRIS][self::CMT_ID] < $headCommentId)
            #     {
            #         $commentEntries = array_slice($commentEntries, 0, $i + 1);
            #         break;
            #     }
            # }

            # $this->insertCommentEntries($commentEntries);

            # if(!$isCrawlingAll) # should be replaced by end timestamp and id
            # {
            #     break;
            # }

            # # update the breakpoint
            # $headComment = array_shift($commentEntries);
            # if(!$headComment)
            # {
            #     continue;
            # }
            # $headCommentTimestamp = $headComment[self::CMTENT_ATTRIS][self::CMT_DATE];
            # $headCommentId = $headComment[self::CMTENT_ATTRIS][self::CMT_ID];

        return merge_xmls(xmls)