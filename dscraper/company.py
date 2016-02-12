import logging

from .scraper import CommentWorker, Distributor
from .utils import CountLatch
from .exporter import FileExporter
from .expections import Scavenger

_logger = logging.getLogger(__name__)

AID = 'AID'
CID = 'CID'

class BaseCompany:
    """Controls the number and operation of workers under the same policy.

    Can be: distributor, scavenger
    """
    def __init__(self, worker_ctor, num_workers, *, scavenger=None, loop):
        self.loop = loop
        self.scavenger = scavenger or Scavenger()
        self._num_workers = num_workers
        self._closed = False
        self._ctor = worker_ctor
        self._workers = set()
        self._latch = CountLatch(loop=self.loop)
        self.success = self.scavenger.success
        self.is_dead = self.scavenger.is_dead

    async def run(self):
        self.hire(self._num_workers)
        await self._latch.wait()

    def hire(self, num=1):
        for _ in range(num):
            worker = self._ctor()
            fut = asyncio.ensure_future(worker.run(), loop=self.loop)
            fut.add_done_callback(lambda x: self._latch.count_down())
            self._workers.add(worker)
        self._latch.count(num)
        self.scavenger.set_recorders_by(num)

    def fire(self, num=1):
        num = min(len(self._workers), num)
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
        self.exporter = exporter or FileExporter()
        ctor = lambda: CommentWorker(distributor=self, exporter=self.exporter,
                                     scavenger=self, fetcher=CIDFetcher(self.loop),
                                     history=history)
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
