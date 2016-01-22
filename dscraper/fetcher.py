__all__ = ('Fetcher', )

import logging
import asyncio
import zlib
import xmltodict as x2d
import json

from .utils import aretry, AutoConnector, get_headers_text, is_response_complete, get_status_code
from .exceptions import HostError, ConnectTimeout, ResponseError, DecodeError, ParseError, MultipleErrors, NoResponseReadError

_logger = logging.getLogger(__name__)


_HOST = 'comment.bilibili.tv'
_PORT = 80
_REQUEST_TEMPLATE = 'GET {{uri}} HTTP/1.1\r\n{headers}\r\n'
_DEFAULT_HEADERS = {
    'Host': _HOST,
    'User-Agent': 'dscraper/1.0'
}
# TODO: switch to backup headers if necessary
_BACKUP_HEADERS = {
# _DEFAULT_HEADERS = {
    'Host': _HOST,
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/600.7.12 (KHTML, like Gecko) Version/8.0.7 Safari/600.7.12',
    'Referer': 'http://www.baidu.com/',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate'
}


class Fetcher:

    def __init__(self, loop=None):
        self.session = Session(_HOST, _PORT, loop)
        self.headers = _DEFAULT_HEADERS
        self.request_template = _REQUEST_TEMPLATE.format(headers=
                            get_headers_text(self.headers))
        self._locking = False

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.close()

    async def open(self):
        try:
            await self.session.connect()
        except ConnectTimeout as e:
            _logger.warning('Failed to connect to the host')
            e.results_in('cannot connect to the host')
            raise

    def close(self):
        self.session.disconnect()

    async def fetch(self, uri):
        """raise HostError, DecodeError
        Has coroutine lock
        """
        if self._locking:
            raise RuntimeError('coroutine already running')
        try:
            self._locking = True
            return await self._fetch(uri)
        finally:
            self._locking = False

    async def _fetch(self, uri):
        # make the request text
        request = self.request_template.format(uri=uri).encode('ascii')

        # try to get the response
        try:
            headers, body = await self.session.get(request)
        except (ConnectTimeout, MultipleErrors) as e:
            _logger.warning('Failed to read from %s', uri)
            raise HostError('cannot read from {}'.format(uri)) from e

        # check the status code
        if get_status_code(headers) == 404:
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
            raise ParseError('content cannot be parsed as XML') from e
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
            raise ParseError('content cannot be parsed as JSON') from e
        return json


class Session(AutoConnector):

    def __init__(self, host, port, loop):
        super().__init__(_CONNECT_TIMEOUT)
        self.host = host
        self.port = port
        self.loop = loop
        self.reader = self.writer = None

    @aretry(HostError, 'connect')
    async def get(self, request):
        # send the request
        self.writer.write(request)
        try:
            await self.writer.drain()
        except ConnectionError as e:
            _logger.info('Connection to the host was reset')
            raise HostError('connection reset') from e

        # read the response
        response = await self.read()
        if not response:
            _logger.debug('No response from the host on %s', request)
            raise NoResponseReadError('no response from the host')

        # disassemble the response to check completeness
        try:
            headers, body = response.split(b'\r\n\r\n', maxsplit=1)
        except ValueError:
            _logger.info('Response from the host was invalid')
            _logger.debug('response: \n%s', response)
            raise ResponseError('invalid response')
        else:
            return (headers, body)

    async def read(self):
        # TODO no data from host time-out / read time-out
        # shield, wait_for
        response = b''
        while not is_response_complete(response):
            chunk = await self.reader.read(16384)
            if not chunk:
                break
            response += chunk
        return response

    async def _open_connection(self):
        if self.writer:
            self.disconnect()
            _logger.debug('Trying to reconnect to the host')
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port, loop=self.loop)

    def disconnect(self):
        self.writer.close()
        self.reader = self.writer = None

_CONNECT_TIMEOUT = 7
_READ_TIMEOUT = 14