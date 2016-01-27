__all__ = ('Scraper', )

import logging


from .fetcher import Fetcher
from .exporter import FileExporter
from .exceptions import (InvalidCid, DscraperError, Watcher)
from .utils import parse_xml, parse_json, merge_xmls, get_all_cids, cid_checker

_logger = logging.getLogger(__name__)

# _cooldown_duration = 1 # TODO adjust according to polite time, not belong here
# _fetcher = None

# async def get(cid, timestamp=0, loop=None):
#     global _fetcher
#     if not _fetcher or loop:
#         _fetcher = Fetcher(loop=loop)

#     async with _fetcher:
#         try:
#             return await _fetcher.fetch_xml(cid, timestamp)
#         except:
#             # TODO
#             raise

# async def scrape(mixed):
#     pass


class Scraper:

    def __init__(self, *, exporter=None, history=True, loop=None, workers=6): # TODO workers = 3?
        # Validate arguments
        if not loop:
            loop = asyncio.get_event_loop()
        if exporter is None:
            exporter = FileExporter(loop=loop)
        if workers <= 0:
            raise ValueError('not a valid number')

        watcher = Watcher()
        self.workers = [Worker(cid_iter=cid_iter, exporter=self.exporter,
                               watcher=watcher, loop=loop, history=history)
                        for _ in range(self.workers)]
        watcher.register(self.workers)

    def scrape_list(self, cids):
        """Input should be an iterable of integers"""
        self._scrape(cid_checker(cids))

    def scrape_range(self, start, end):
        if start <= 0 or end < start:
            raise ValueError('Not a valid range: [{} - {}]'.format(start, end))
        self._scrape(range(start, end + 1))

    def scrape_mixed(self, cids):
        """Input should be an iterable of integers and/or of iterables of integers"""
        self._scrape(cid_checker(get_all_cids(cids)))

    def _scrape(self, args):
        pass
        # start working
        # try:
            # TODO
            # # self.loop.run_until_complete(gather(workers.scrape_all()))
            # await gather
        # except :
            # raise

    # def _start(self):
    #     self.exporter.open()

    # def _end():
    #     self.exporter.close()
    #     pass


class Worker:

    def __init__(self, cid_iter, exporter, watcher, loop, history):
        self.targets = cid_iter
        self.exporter = exporter
        self.fetcher = Fetcher(loop=loop)
        self.watcher = watcher
        self.history = history

    async def scrape_all(self):
        async with self.fetcher:
            while not self.watcher.is_dead():
                # TODO update progress, already scraped, current scraping
                try:
                    self._set_next_cid()
                    await self._scrape_next()
                except StopIteration:
                    break
                except DscraperError as e:
                    self.watcher.damage(e, self)
                except:
                    self.watcher.unexpected_damage(self)
                else:
                    self.watcher.heal()
                # TODO relax according to polite time

        # TODO sum up the result, report
        if self.watcher.is_dead():
            _logger.critical('Worker is down due to too many expections!')
        else:
            pass

    async def _scrape_next(self):
        # Get the data
        text = await self.fetcher.fetch_comments(self.cid)
        # Get the history data
        if self.history:
            root = parse_xml(text)
            # Continue only if the latest data contains comments no less than it could contain at maximum
            elimit = root.find('maxlimit')
            if elimit:
                limit = int(elimit.text)
                num_comments = len(root.findall('d') or [])
                if num_comments >= limit:
                    await self._scrape_history(root)
        # Export all data scraped in this round
        merge_xmls()
        await self.exporter.dump(self.cid, root, )

    def _set_next_cid(self):
        cid = next(self.targets)
        try:
            cid = int(cid)
        except TypeError:
            raise InvalidCid('Invalid cid from input: an integer is required, not \'{}\''.format(type(cid).__name__))
        if cid <= 0:
            raise InvalidCid('Invalid cid from input: a positive integer is required')
        self.cid = cid

    async def _scrape_history(self, root):
        # TODO
        roll_dates = parse_json(await self.fetcher.fetch_rolldate(self.cid))
        pass
