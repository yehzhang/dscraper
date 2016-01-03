__all__ = ()

import xmltodict as x2d
import json
import random

from . import utils
from .fetcher import Fetcher
from .exceptions import (ParseError, DataError, ConnectionError, InvalidCid,
    _MAX_HEALTH, _HEALTH_REGEN, _mark_recorders)

logger = utils.get_logger(__name__)

# _cooldown_duration = 1 # TODO adjust according to polite time, not belong here
_fetcher = None

async def get(cid, timestamp=0, loop=None):
    global _fetcher
    if not _fetcher:
        _fetcher = Fetcher(loop=loop)
    elif loop:
        _fetcher.set_loop(loop)
    await _fetcher.connect()

    try:
        return await _get_xml(cid, timestamp)
    except:
        # TODO
        raise
    finally:
        _fetcher.disconnect()

class Scraper:

    def __init__(self, *, exporter=None, loop=None, workers=1, history=True, exhaust=False):
        self.loop = loop or asyncio.get_event_loop()
        # TODO exporter
        if workers <= 0:
            raise InvalidCid('not a valid number')
        self.workers = workers
        self.stat = {'health': _MAX_HEALTH}

    def scrape_range(self, start, end):
        if start <= 0 or end < start:
            raise ValueError('not a valid range')
        cid_iter = (cid for cid in range(start, end + 1))
        self.scrape_list(cid_iter)

    def scrape_list(self, cid_iter):
        """Input should be an iterable of integers, either consecutive or discrete"""
        workers = [_Worker(cid_iter, exporter=, self.stat, loop=self.loop) for _ in range(self.workers)]
        # TODO explain balance
        _mark_recorders(workers)
        # start working
        try:
            # TODO
            # self.loop.run_until_complete(gather(workers.scrape_all()))
        except InvalidCid:
            raise

    def scrape_mixed(self, cid_iters):
        """Input should be an iterable of iterables of integers"""
        cid_iter = (cid for cid_iter in cid_iters for cid in cid_iter)
        self.scrape_list(cid_iter)


class _Worker:

    def __init__(self, cid_iter, exporter, stat, loop=None):
        self.list = cid_iter
        self.exporter = exporter
        self.fetcher = Fetcher(loop=loop)
        self.stat = stat

    async def scrape_all(self):
        try:
            await self.fetcher.connect()
            while True:
                if self._health <= 0:
                    break
                try:
                    self._fetch_next()
                except StopIteration:
                    break
                except (ConnectionError, DataError, InvalidCid) as e:
                    self._health -= e.damage
                else:
                    self._health += _HEALTH_REGEN
        finally:
            self.fetcher.disconnect()


    async def _fetch_next(self):
        next = next(self.list)
        try:
            if next <= 0:
                raise ValueError
        except (ValueError, TypeError) as e:
            _logger.warning('Invalid input of cid: %e', e)
            raise InvalidCid
        text = fetcher.fetch(next)
        # TODO xml = ..., history = ...
        #   exporter.export(all)

    @property
    def _health(self):
        return self.stat['health']

    @_health.setter
    def _health(self, value):
        if not self.is_recorder:
            return
        if value > 0:
            self.stat['health'] = min(
                self.stat['health'] + value, _MAX_HEALTH)
        else:
            self.stat['health'] -= value

async def _get_xml(fetcher, cid, timestamp=0):
    if timestamp is 0:
        uri = '/{}.xml'.format(cid)
    else:
        uri = '/dmroll,{},{}'.format(timestamp, cid)
    # expected outcome:
    #   valid XML string, √
    #   no outcome / 404 not found, √
    #   XML string containing a single element with 'error' as content, or
    #   XML string with invalid characters
    # exception:
    #   connection timed out
    #   cannot decode
    text = await fetcher.fetch(uri)

    if not text:
        return None
    try:
        xml = x2d.parse(text)
    except Exception as e: # TODO what exception means what?
        _logger.warning('Failed to parse the content as XML at cid %s for %s', cid, e)
        raise ParseError('the content cannot be parsed as XML')
    # TODO
    # if :
    #     pass
    return xml

async def _get_json(fetcher, cid):
    uri = '/rolldate,{}'.format(cid)
    # expected outcome:
    #   valid JSON string, √
    #   no outcome / 404 not found, √
    # exception:
    #   connection timed out
    #   cannot decode
    text = await fetcher.fetch(uri)

    if not text:
        return None
    try:
        json = json.loads(text)
    except json.JSONDecodeError as e:
        _logger.warning('Failed to parse the content as JSON at cid %s for %s', cid, e)
        raise ParseError('the content cannot be parsed as JSON')
    return json