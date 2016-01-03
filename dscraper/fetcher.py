__all__ = ('get', )

import asyncio
import xmltodict as x2d
import zlib

from . import utils

_BACKUP_HEADERS = {
    # 'Referer': 'http://www.baidu.com/',
    'Host': 'comment.bilibili.com',
    'Connection': 'keep-alive', # does it work?
    'User-Agent': 'dscraper/1.0',
    # 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/600.7.12 (KHTML, like Gecko) Version/8.0.7 Safari/600.7.12',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate'
}

# _cooldown_duration = 1 # TODO adjust according to polite time, not belong here

_logger = utils.get_logger(__name__)
_fetcher = None

async def get(cid, timestamp=0, loop=None):
    global _fetcher
    if not _fetcher:
        _fetcher = _Fetcher(loop=loop)
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



async def _get_xml(fetcher, cid, timestamp=0):
    if timestamp is 0:
        uri = '/{}.xml'.format(cid)
    else:
        uri = '/dmroll,{},{}'.format(timestamp, cid)

    text = await fetcher.fetch(uri) # possible return: exception on connection failure, html string, xml string containing a single element with 'error' as content or containing invalid characters of xml
    try:
        xml = x2d.parse(raw)
    except Exception as e:
        raise e
    return xml


_HOST = 'comment.bilibili.com'
_PORT = 80
_REQUEST_TEMPLATE = 'GET {{uri}} HTTP/1.1\r\n{headers}\r\n'
_DEFAULT_HEADERS = {
    'Host': _HOST
}
_ORDINAL = (None, 'first', 'second', 'third')
_RETRIES = 3

class _Fetcher:

    connect_timeout = 7
    # read_timeout = 14

    def __init__(self, loop=None):
        self.loop = loop
        self._set_headers(_DEFAULT_HEADERS)
        self.decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
        self.reader = self.writer = None

    async def connect(self):
        self.disconnect()
        for tries in range(1, _RETRIES + 1):
            try:
                self.reader, self.writer = await asyncio.wait_for(
                    asyncio.open_connection(_HOST, _PORT, loop=self.loop),
                    connect_timeout)
            except asyncio.TimeoutError:
                _logger.info('Connecting timed out for the %s time', _ORDINAL[tries])
            else:
                _logger.debug('Connection was established')
                return
        _logger.warning('Failed to connect to the host after %d tries', _RETRIES)
        raise ConnectTimeout('connecting to the host timed out')

    async def fetch(self, uri):
        # send the request
        if not uri:
            raise RuntimeError('no uri given')
        await self._ensure_connection()
        request = self.template.format(uri=uri).encode('ascii')
        writer.write(request)
        body = None

        # read the response and extract headers and body
        _logger.debug('Start reading from the url')
        response = b''
        while True:
            chunk = await reader.read(1024)
            if not chunk:
                break
            response += chunk

            # determine the length of response by looking for Content-Length
            parts = response.split(b'\r\n\r\n', maxsplit=1)
            if len(parts) is not 2:
                continue
            headers, body = parts
            match = re.search(b'Content-Length: (\d+)\r\n', headers)
            if utils.assert_false(match, _logger,
                                 'Invalid header got at %s', uri):
                continue
            # if Content-Length is found, read bytes of the same length only,
            # which are supposed to be the body of response
            content_length = int(match.group(1))
            if utils.assert_false(content_length > 0, _logger,
                                 'Invalid Content-Length got at %s', uri):
                continue
            while len(body) < content_length:
                chunk = await reader.read(16384)
                if not chunk:
                    break
                body += chunk
            break
        if not body:
            headers, body = response.split(b'\r\n\r\n', maxsplit=1)
        _logger.debug('loop breaker of read: content_length: %d, body length: %d',
                      content_length, len(body))

        # produce output
        if self.get_status_code(headers) is 404:
            _logger.info('Page of %s is not found', uri)
            return None
        try:
            return self.decompressor.decompress(body).decode()
        except (zlib.error, UnicodeDecodeError) as e:
            _logger.warning('Failed to decode the raw content from %s for %s', uri, e)
            raise DecodeError('the raw content cannot be decoded')

    def disconnect(self):
        if self.reader:
            self.reader.close()
        if self.writer:
            self.writer.close()

    def set_loop(loop):
        self.loop = loop
        if not self._is_closing():
            self.connect()

    @staticmethod
    def get_status_code(raw):
        match = re.search(b'HTTP/1.1 (\d+) ', raw)
        if match:
            return int(match.group(1))
        return None

    def _set_headers(self, headers):
        self.headers = headers
        self.template = _REQUEST_TEMPLATE.format(headers=
            ''.join('{}:{}\r\n'.format(k, v) for k, v in headers.items()))

    async def _ensure_connection(self):
        if self._is_closing():
            await self.connect()
            _logger.debug('Connection was broken unexpectedly')

    def _is_closing():
        return not (self.reader and self.writer) or self.writer.transport.is_closing()


class DscraperError(OSError):
    pass

class ConnectTimeout(DscraperError):
    """Attempts to connect to the host timed out after multiple retries.

    Do not retry until the problem is fixed, which is probably that
    your IP is blocked by the host, or your computer is disconnected from the Internet.
    """

class DecodeError(DscraperError, ValueError):
    """The bytes read from the given connection cannot be decoded"""

class ParseError(DscraperError, ValueError):
    """The given string cannot be parsed as XML or JSON"""