import logging

from .scraper import Worker, Distributor
from .expections import Scavenger

_logger = logging.getLogger(__name__)

class Company:
    """Controls the number and the state of workers under the same host.
    """
    def __init__(self, time_config, *, loop):
        self.loop = loop
        self.distributor = Distributor(loop=self.loop)
        self.scavenger = Scavenger()
        # self.controller = FlowController(*time_config, loop=self.loop)
        self._closed = False
        self._tasks = set()
        self._tasks = set()

    def run(self):
        # TODO
        pass

    def hire(num=1):
        for _ in range(num):
            worker = Worker(self)
            fut = asyncio.ensure_future(worker.run(), loop=self.loop)
            self._tasks.add((worker, fut))
            # TODO return futures so that they can be waituntiled

    def fire(num=1, force=False):
        i = 0
        for task in self._tasks:
            if i >= num:
                break
            worker, fut = task
            if force:
                # TODO does cancel here affect waituntil?
                fut.cancel()
            else:
                worker.stop()
            i += 1

    async def claim(self):
        cid = self.distributor.claim()
        # await self.controller.wait()
        if self._closed:
            raise StopIteration('call it a day')
        return cid

    def failure(self, worker, e):
        if e is None:
            self.scavenger.unexpected_damage(worker)
        else:
            self.scavenger.damage(e, worker)
        if self.scavenger.is_dead():
            _logger.critical('Company is down due to too many expections!')
            self.close()
            # TODO also collect items yet to be scraped

    def success():
        self.scavenger.heal()

    def post(self, item):
        self.distributor.post(item)

    def close(self):
        self.distributor.drop()
        for worker in self.workers:
            worker.stop()
        self._closed = True