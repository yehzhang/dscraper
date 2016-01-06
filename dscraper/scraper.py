__all__ = ()

from . import utils
from .fetcher import Fetcher
from .exceptions import (InvalidCid, DscraperError, Life)

logger = utils.get_logger(__name__)

# _cooldown_duration = 1 # TODO adjust according to polite time, not belong here
_fetcher = None

async def get(cid, timestamp=0, loop=None):
    global _fetcher
    if not _fetcher or loop:
        _fetcher = Fetcher(loop=loop)

    async with _fetcher:
        try:
            return await _fetcher.fetch_xml(cid, timestamp)
        except:
            # TODO
            raise

class Scraper:

    def __init__(self, *, exporter=None, history=True, exhaust=False, loop=None, workers=6):
        self.loop = loop or asyncio.get_event_loop()

        # TODO exporter
        if exporter is None:
            pass

        if workers <= 0:
            raise ValueError('not a valid number')
        self.life = Life()
        self.workers = [_Worker(cid_iter=cid_iter, exporter=, life=self.life,
                                loop=self.loop, history=history, exhaust=exhaust)
                        for _ in range(self.workers)]
        self.life.set_recorders(self.workers)

    def scrape_range(self, start, end):
        if start <= 0 or end < start:
            raise ValueError('not a valid range')
        cid_iter = (cid for cid in range(start, end + 1))
        self.scrape_list(cid_iter)

    def scrape_list(self, cid_iter):
        """Input should be an iterable of integers"""
        # start working
        try:
            # TODO
            # self.loop.run_until_complete(gather(workers.scrape_all()))
        except:
            raise

    def scrape_mixed(self, cid_iters):
        """Input should be an iterable of integers and/or of iterables of integers"""
        cid_iter = (cid for cid_iter in cid_iters for cid in cid_iter)
        self.scrape_list(cid_iter)


class _Worker:

    def __init__(self, cid_iter, exporter, life, loop, history, exhaust, threads=1):
        self.list = cid_iter
        self.exporter = exporter
        self.fetcher = Fetcher(loop=loop)
        self.life = life
        self.history = history
        self.exhaust = exhaust
        # self.threads = threads

    async def scrape_all(self):
        async with self.fetcher:
            while True:
                if self.life.is_dead():
                    break
                try:
                    self._scrape_next()
                except StopIteration:
                    break
                except DscraperError as e:
                    self.life.damage(e, self)
                else:
                    self.life.heal()

    async def _scrape_next(self):
        next = next(self.list)
        try:
            if next <= 0:
                raise ValueError
        except (ValueError, TypeError) as e:
            _logger.warning('Invalid input of cid: %s' % e)
            raise InvalidCid
        text = self.fetcher.fetch(next)
        # history, exhause
        # history and FileExporter => exhaust
        # history and other Exporter => exhaust or not exhaust
        # not history => not exhaust
        # TODO xml = ..., history = ...
        #   exporter.export(all)
