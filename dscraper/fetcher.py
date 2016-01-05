__all__ = ('Fetcher', )

import asyncio
import re
import zlib
import xmltodict as x2d
import json

from .utils import get_logger, alock
from .exceptions import ConnectionError, ConnectTimeout, ResponseError, DecodeError, ParseError

_logger = get_logger(__name__)

_HOST = 'comment.bilibili.com'
_PORT = 80
_REQUEST_TEMPLATE = 'GET {{uri}} HTTP/1.1\r\n{headers}\r\n'
_DEFAULT_HEADERS = {
    'Host': _HOST,
    'User-Agent': 'dscraper/1.0'
}
# TODO: switch to backup headers if necessary
_BACKUP_HEADERS = {
    'Host': _HOST,
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/600.7.12 (KHTML, like Gecko) Version/8.0.7 Safari/600.7.12',
    'Referer': 'http://www.baidu.com/',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate'
}
_CONNECT_TIMEOUT = 7
_READ_TIMEOUT = 14
_RETRIES = 2

class Fetcher:

    def __init__(self, loop=None):
        self.connected = False
        self.loop = loop
        self.headers = _DEFAULT_HEADERS
        self.template = _REQUEST_TEMPLATE.format(headers=
                            _get_headers_text(self.headers))

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.disconnect()

    async def connect(self):
        self.disconnect()
        # if connection timed out, retry
        for tries in range(1, _RETRIES + 1):
            try:
                self.reader, self.writer = await asyncio.wait_for(
                    asyncio.open_connection(_HOST, _PORT, loop=self.loop),
                    _CONNECT_TIMEOUT)
            except asyncio.TimeoutError:
                _logger.info('Connection timed out')
            else:
                self.connected = True
                return
        _logger.warning('Failed to connect to the host after %d tries', _RETRIES)
        raise ConnectTimeout('connection to the host timed out')

    @alock
    @asyncio.coroutine
    def fetch(self, uri):
        """raise ConnectionError(, ConnectTimeout), DecodeError"""
        request = self.template.format(uri=uri).encode('ascii')

        # if ConnectionError occurred, retry
        retries = 0
        while True:

            try:
                # send the request
                self.writer.write(request)
                try:
                    yield from self.writer.drain()
                except ConnectionResetError:
                    _logger.info('Connection to %s was reset', uri)
                    raise ConnectionError('connection reset')

                # read the response
                response = yield from self._read()

                # disassemble the response
                try:
                    headers, body = response.split(b'\r\n\r\n', maxsplit=1)
                except ValueError:
                    _logger.info('Response from %s was invalid', uri)
                    _logger.debug('response: \n%s', response)
                    raise ResponseError('invalid response')

            except ConnectionError as e:
                # guarding condition
                if retries >= _RETRIES:
                    _logger.warning('Failed to read from %s after %d retries', uri, _RETRIES)
                    raise ConnectionError('cannot read from {}'.forma(uri)) from e

            else:
                break

            retries += 1
            _logger.info('Trying to solve this problem by reconnecting to the host')
            yield from self.connect()

        # check the status code
        match = re.search(b'HTTP/1.1 (\d+) ', headers)
        if match and int(match.group(1)) == 404:
            _logger.info('%s is a 404 page', uri)
            return None

        # inflate and decode the body
        try:
            inflated = zlib.decompressobj(-zlib.MAX_WBITS).decompress(body)
            return inflated.decode()
        except (zlib.error, UnicodeDecodeError) as e:
            _logger.warning('Failed to decode the data from %s: %s', uri, e)
            _logger.debug('cannot decode: \n%s', body)
            raise DecodeError('cannot decode the response') from e

    async def fetch_comments(self, cid, timestamp=0):
        if timestamp == 0:
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
        text = await self.fetch(uri)

        if not text:
            return None
        try:
            xml = x2d.parse(text)
        except Exception as e: # TODO what exception means what?
            _logger.warning('Failed to parse the content as XML at cid %s: %s', cid, e)
            raise ParseError('content cannot be parsed as XML')
        # TODO
        # if :
        #     pass
        return xml

    async def fetch_rolldate(self, cid):
        uri = '/rolldate,{}'.format(cid)
        # expected outcome:
        #   valid JSON string, √
        #   no outcome / 404 not found, √
        # exception:
        #   connection timed out
        #   cannot decode
        text = await self.fetch(uri)

        if not text:
            return None
        try:
            json = json.loads(text)
        except json.JSONDecodeError as e:
            _logger.warning('Failed to parse the content as JSON at cid %s: %s', cid, e)
            raise ParseError('content cannot be parsed as JSON')
        return json

    def disconnect(self):
        if self.connected:
            self.writer.close()
            self.connected = False

    async def _read(self):
        # _logger.debug('read start')
        # TODO no data from host time-out / read time-out
        # shield, wait_for
        response = b''
        while not _is_response_complete(response):
            chunk = await self.reader.read(16384)
            if not chunk:
                break
            response += chunk
        # _logger.debug('read over')
        return response

def _get_headers_text(headers):
    return ''.join('{}:{}\r\n'.format(k, v) for k, v in headers.items())

def _is_response_complete(raw):
    """Locate the end of response by looking for Content-Length.
    If Content-Length is found in the response, read bytes of the same length only,
    which are supposed to be the body of response.
    """
    parts = raw.split(b'\r\n\r\n', maxsplit=1)
    if len(parts) == 2:
        headers, upperbody = parts
        match = re.search(b'Content-Length: (\d+)\r\n', headers)
        if match:
            content_length = int(match.group(1))
            return len(upperbody) == content_length
    return False

