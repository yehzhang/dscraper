__all__ = ('Fetcher', )

import logging
import asyncio

from .utils import alock, AutoConnector, get_headers_text, is_response_complete, get_status_code, inflate_and_decode
from .exceptions import HostError, ConnectTimeout, ResponseError, DecodeError, MultipleErrors, NoResponseReadError, PageNotFound

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
        await self.session.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.session.disconnect()

    async def open(self):
        await self.session.connect()

    async def close(self):
        await self.session.disconnect()

    @alock
    async def fetch(self, uri):
        """raises HostError, DecodeError, PageNotFound"""
        if self.session.disconnected():
            raise RuntimeError('fetcher is not opened yet')
        # make the request text
        request = self.request_template.format(uri=uri).encode('ascii')
        # try to get the response
        try:
            headers, body = await self.session.get(request)
        except (ConnectTimeout, MultipleErrors) as e:
            raise HostError('Failed to read from the host') from e
        # check the status code
        if get_status_code(headers) == 404:
            # TODO after visiting 404 page, the reader keeps blocking without break immediately, yet the host says keep-alive, resulting in connection timeout. should fix it by improve read() terminating detection instead of wait for overtime and reconnect
            await self.session.disconnect()
            await self.session.connect()
            raise PageNotFound('Fetching a 404 page')
        # inflate and decode the body
        return inflate_and_decode(body)

    async def fetch_comments(self, cid, timestamp=0):
        if timestamp == 0:
            uri = '/{}.xml'.format(cid)
        else:
            uri = '/dmroll,{},{}'.format(timestamp, cid)
        return await self.fetch(uri)

    async def fetch_rolldate(self, cid):
        uri = '/rolldate,{}'.format(cid)
        return await self.fetch(uri)


class Session(AutoConnector):

    def __init__(self, host, port, loop):
        super().__init__(_CONNECT_TIMEOUT, loop, 'Failed to open connection to the host')
        self.host = host
        self.port = port
        self.loop = loop
        self.reader = self.writer = None

    async def get(self, request):
        errors = []
        for tries in range(_READ_RETRIES + 1):
            try:
                return await self._get(request)
            except HostError as e:
                errors.append(e)
                await self.connect()
        # TODO if same errors, return the error itself
        raise MultipleErrors(errors)

    async def _get(self, request):
        # send the request
        self.writer.write(request)
        try:
            await self.writer.drain()
        except ConnectionError as e:
            raise HostError('Connection to the host was reset') from e

        # read the response
        response = await self.read()
        if not response:
            _logger.debug('No response from the host on %s', request)
            raise NoResponseReadError('no response from the host')

        # disassemble the response to check completeness
        try:
            headers, body = response.split(b'\r\n\r\n', maxsplit=1)
        except ValueError:
            _logger.debug('response: \n%s', response)
            raise ResponseError('Response from the host was invalid')
        else:
            return (headers, body)

    async def read(self):
        # TODO no data from host time-out / read time-out
        # TODO efficient check body length, check status code here
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
            await self.disconnect()
            _logger.debug('Trying to reconnect to the host')
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port, loop=self.loop)

    async def disconnect(self):
        self.writer.close()
        self.reader = self.writer = None

    def disconnected(self):
        return self.writer is None

_CONNECT_TIMEOUT = 7
_READ_TIMEOUT = 14
_READ_RETRIES = 2