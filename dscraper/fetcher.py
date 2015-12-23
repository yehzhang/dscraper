__all__ = ('get', 'use_faster_host')

import asyncio
import aiohttp
import xmltodict as x2d
import atexit

from . import utils

_HOSTS = (
    'http://comment.bilibili.com',
    'http://comment.bilibili.tv'
)
_DEFAULT_HEADER = {
    # 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/600.7.12 (KHTML, like Gecko) Version/8.0.7 Safari/600.7.12',
    'Referer': 'www.baidu.com'
}
_host = _HOSTS[0]
_headers = _DEFAULT_HEADER
_timeout = 5 # connect timeout
_cooldown_duration = 1 # TODO adjust according to polite time

_logger = utils.get_logger(__name__)


async def get(cid, timestamp=0, *, timeout=5, headers=None):
    _timeout = timeout
    if headers is not None:
        set_headers(headers)
    _start_session()
    try:
        return await _get_xml(cid, timestamp)
    except:
        # TODO
        raise
    finally:
        _close_session()

def set_headers(headers):
    global _headers
    _headers = headers

# TODO
# comment.bilibili.tv vs comment.bilibili.com
def use_faster_host():
    if _is_fast:
        return
    _is_fast = True
    raise NotImplementedError

_is_fast = False


async def _get_xml(cid, timestamp=0):
    if timestamp is 0:
        url = '%s/%d.xml' % (host, cid)
    else:
        url = '%s/dmroll,%d,%d' % (host, timestamp, cid)

    raw = await _get_url_contents(url) # possible return: exception on connection failure, html string, xml string containing a single element with 'error' as content or containing invalid characters of xml
    xml = x2d.parse(raw)
    return xml

async def _get_url_contents(url):
    if not url:
        raise RuntimeError('No url given')
    _ensure_session()

    _logger.debug('Start getting url content')
    for tries in range(1, _RETRY + 1):
        try:
            with aiohttp.Timeout(_timeout):
                async with _session.get(url) as resp:
                    raw = await resp.read()
        except Exception as e:
            _logger.info('Failed to get url content for the %s time for %s', _ORDINAL[tries], e)
        else:
            _logger.debug('Finish getting url content: %s', raw)
            return raw
    _logger.warning('Failed to get content of %s', url)
    raise RuntimeError('failed to get content')

_ORDINAL = (None, '1st', '2nd', '3rd')
_RETRY = 3

def _ensure_session():
    if not _session or _session.closed:
        _start_session()
        _logger.debug('Session was closed unexpectedly')

def _start_session():
    _close_session()
    global _session
    _session = aiohttp.ClientSession(headers=_headers)
    _logger.debug('Session was started')

def _close_session():
    global _session
    if _session:
        if not _session.closed:
            _session.close()
            _logger.debug('Session was closed')
        _session = None

_session = None

