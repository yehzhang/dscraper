import utils
logger = utils.get_logger(__name__)

import asyncio
from aiohttp import ClientSession, Timeout


_HOSTS = (
    'comment.bilibili.com',
    'comment.bilibili.tv'
)
_DEFAULT_HEADER = {
	# 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/600.7.12 (KHTML, like Gecko) Version/8.0.7 Safari/600.7.12',
	'Referer': 'www.baidu.com'
}
_host = _HOSTS[0]
_header = _DEFAULT_HEADER
_timeout = 5 # connect timeout
_cooldown_duration = 1 # TODO adjust according to polite time


async def get(cid, timestamp=0, *, timeout=5, headers=None):
	_timeout = timeout
	if headers is not None:
		_ensure_connection(headers)

	return await _get_xml(cid, timestamp)

# TODO ToTest
def config(**kwargs):
	auto = kwargs.pop('autohost', None)
	if auto:
		_connect_faster_host()

async def _get_xml(cid, timestamp=0):
	if timestamp is 0:
		url = '%s/%d.xml' % (host, cid)
	else
		url = '%s/dmroll,%d,%d' % (host, timestamp, cid)

	$raw = await _get_url_contents(url)
	# TODO

async def _get_url_contents(url):
	if not url:
		raise RuntimeError('No url given')
	if not _ensure_connection() and _count_requests is not 0:
		logger.debug('Session was closed unexpectedly')
	_count_requests += 1

	logger.debug('Start getting url content')
	for tries in range(1, _RETRY + 1):
		try:
			with Timeout(timeout):
				async with _session.get(url) as resp:
					raw = await resp.read()
		except Exception as e:
			logger.info('Failed to get url content for the %s time for %s', _ORDINAL[tries], e)
		else:
			logger.debug('Finish getting url content: %s', raw)
			return raw
	logger.warning('Failed to request content of %s', url)
	raise

_ORDINAL = (None, '1st', '2nd', '3rd')
_RETRY = 3
_session = None
_count_requests = 0

def _ensure_connection(headers=_header):
	if not _session or _session.closed:
		_session = ClientSession(headers=headers)
		return False
	return True

# TODO
def _connect_faster_host():
	# comment.bilibili.tv vs comment.bilibili.com
	if _tested:
		return
	_tested = True
	pass

_tested = False
