__all__ = ('Scraper', )

import logging


from .fetcher import Fetcher
from .exporter import FileExporter
from .exceptions import (InvalidCid, DscraperError, Life)

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
        # TODO exporter
        if exporter is None:
            pass
        if workers <= 0:
            raise ValueError('not a valid number')

        life = Life()
        # Use exhaustive scraping only when scraping the history and exporting to files
        exhaust = history is True and isinstance(exporter, FileExporter)
        self.workers = [Worker(cid_iter=cid_iter, exporter=self.exporter,
                               life=life, loop=loop,
                               history=history, exhaust=exhaust)
                        for _ in range(self.workers)]
        life.set_recorders(self.workers)

    def scrape(self, args):
        pass

    async def scrape_range(self, start, end):
        if start <= 0 or end < start:
            raise ValueError('not a valid range')
        cid_iter = (cid for cid in range(start, end + 1))
        await self.scrape_list(cid_iter)

    async def scrape_list(self, cid_iter):
        """Input should be an iterable of integers"""
        # start working
        # try:
            # TODO
            # # self.loop.run_until_complete(gather(workers.scrape_all()))
            # await gather
        # except :
            # raise

    async def scrape_mixed(self, cid_iters):
        """Input should be an iterable of iterables of integers"""
        cid_iter = (cid for cid_iter in cid_iters for cid in cid_iter)
        await self.scrape_list(cid_iter)

    # def _start(self):
    #     self.exporter.open()

    # def _end():
    #     self.exporter.close()
    #     pass


class Worker:

    def __init__(self, cid_iter, exporter, life, loop, history, exhaust):
        self.targets = cid_iter
        self.exporter = exporter
        self.fetcher = Fetcher(loop=loop)
        self.life = life
        self.history = history
        self.exhaust = exhaust

    async def scrape_all(self):
        async with self.fetcher:
            while True:
                # TODO update progress, already scraped, current scraping
                if self.life.is_dead():
                    break
                try:
                    self._set_next_cid()
                except StopIteration:
                    break
                except InvalidCid:
                    self.life.damage(e, self)
                    continue
                try:
                    await self._scrape_next()
                except DscraperError as e:
                    _logger.info('Skip scraping cid %d', self.cid)
                    self.life.damage(e, self)
                else:
                    self.life.heal()

                # TODO relax according to polite time
        # TODO sum up the result, report

    async def _scrape_next(self):
        # Update the exporter's state
        self.exporter.listen(self.cid)
        # Get the data and export it
        text = await self.fetcher.fetch_comments(self.cid, timestamp)
        await self.exporter.add(text)
        # Decide whether to scrape the history
        if self.history:
            root = parse_xml(text)
            # Continue only if the latest data contains comments no less than it could at maximum
            elimit = root.find('maxlimit')
            if elimit:
                limit = int(elimit.text)
                num_comments = len(root.findall('d') or [])
                if num_comments >= limit:
                    if self.exhaust:
                        await self._scrape_history_ex()
                    else:
                        await self._scrape_history(root)
        # Commit all data scraped in this round
        await self.exporter.commit()

    def _set_next_cid(self):
        cid = next(self.targets)
        try:
            cid = int(cid)
            if cid <= 0:
                raise ValueError('negative integer')
        except ValueError as e:
            _logger.warning('Invalid cid from input: %s', e)
            raise InvalidCid('a positive integer is required')
        except TypeError as e:
            message = 'an integer is required, not \'{}\''.format(type(cid).__name__)
            _logger.warning('Invalid cid from input: %s', message)
            raise InvalidCid(message)
        self.cid = cid

    async def _scrape_history(self, root):
        # TODO
        pass

    async def _scrape_history_ex(self):
        roll_dates = parse_json(await self.fetcher.fetch_rolldate(self.cid))
