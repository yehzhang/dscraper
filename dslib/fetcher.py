import time

import utils
logger = utils.get_logger(__name__)

from requests import Session
s = Session()

headers = {
	# 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/600.7.12 (KHTML, like Gecko) Version/8.0.7 Safari/600.7.12',
	'Referer': 'www.baidu.com'
}
s.headers = headers

connect_timeout = 5
read_timeout = 10
cooldown_duration = 1 # TODO adjust according to polite time

host = 'comment.bilibili.com'
alt_hosts = set([
    'comment.bilibili.com',
    'comment.bilibili.tv'
])

def get_xml(cid):
	return _get_xml(cid, 0)

def _get_xml(cid, timestamp):
	pass

# TODO ToTest
def config(**kwargs):
	# TODO type of exporter
	auto = kwargs.pop('autohost', None)
	if auto:
		_connect_faster_host()
	cto = kwargs.pop('connect_timeout', None)
	if cto:
		connect_timeout = cto
	rto = kwargs.pop('read_timeout', None)
	if rto:
		read_timeout = rto
	headers.update(kwargs) # TODO strip invalid key-values

def _get_url_contents(url):
	if not url:
		return None
	tries = 0
	while True:
		try:
			logger.debug('Start getting url content')
			resp = s.get(url, timeout=(connect_timeout, read_timeout))
		except Exception as e:
			tries += 1
			logger.info('Failed to get url content for the %s time', _ORDINAL[tries])
			if tries < _RETRY:
				time.sleep(cooldown_duration)
				logger.debug('Retrying')
			else:
				logger.warning('Failed to request content of %s for %s', url, e)
				return None
		else:
			content = resp.content # TODO encoding
			if not content:
				# TODO raise, log
				pass
			logger.debug('Finish getting url content: %s', content)
			return content

_RETRY = 3
_ORDINAL = (None, '1st', '2nd', '3rd')


# TODO
def _connect_faster_host():
	# comment.bilibili.tv vs comment.bilibili.com
	if _tested:
		return
	_tested = True
	pass

_tested = False
