__all__ = ('Fetcher', )

import logging
import asyncio
import re

from .utils import alock, AutoConnector, get_headers_text, get_status_code, inflate_and_decode
from .exceptions import HostError, ConnectTimeout, ResponseError, DecodeError, MultipleErrors, NoResponseReadError, PageNotFound

_logger = logging.getLogger(__name__)

_HOST = 'comment.bilibili.tv'
_PORT = 80

class Fetcher:

    headers = {
        'User-Agent': 'dscraper/1.0'
    }
    _REQUEST_TEMPLATE = 'GET {{uri}} HTTP/1.1\r\n{headers}\r\n'
    # TODO: switch to backup headers if necessary
    _BACKUP_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/600.7.12 (KHTML, like Gecko) Version/8.0.7 Safari/600.7.12',
        'Referer': 'http://www.baidu.com/',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive'
    }
    def __init__(self, loop=None, *, host=_HOST, port=_PORT, headers=None):
        self.session = Session(host, port, loop)
        if headers:
            self.headers = headers
        self.headers['Host'] = host
        self.template = self._REQUEST_TEMPLATE.format(headers=
                                                              get_headers_text(self.headers))

    async def __aenter__(self):
        await self.session.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.session.disconnect()

    async def open(self):
        await self.session.connect()

    async def close(self):
        await self.session.disconnect()

    async def get(self, uri):
        """
        :param string uri: the URI to fetch content from
        raises HostError, DecodeError, PageNotFound
        """
        # make the request text
        request = self.template.format(uri=uri).encode('ascii')
        # try to get the response
        try:
            headers, body = await self.session.get(request)
        except (ConnectTimeout, MultipleErrors) as e:
            raise HostError('failed to read from the host') from e
        except AttributeError as e:
            raise RuntimeError('fetcher is not opened yet') from e
        # check the status code
        if get_status_code(headers) == 404:
            raise PageNotFound('fetching a 404 page')
        # inflate and decode the body
        return inflate_and_decode(body)

    async def get_comments(self, cid, timestamp=0):
        if timestamp == 0:
            uri = '/{}.xml'.format(cid)
        else:
            uri = '/dmroll,{},{}'.format(timestamp, cid)
        return await self.get(uri)

    async def get_rolldate(self, cid):
        uri = '/rolldate,{}'.format(cid)
        return await self.get(uri)

_DEFAULT_TIMEOUT = (3, 14)

class Session(AutoConnector):

    _READ_RETRIES = 2
    _PATTERN_TE = re.compile(b'Transfer-Encoding: chunked\r\n')
    _PATTERN_CL = re.compile(b'Content-Length: (\d+)\r\n')

    def __init__(self, host, port, timeout=_DEFAULT_TIMEOUT, loop):
        self.connect_timeout, self.read_timeout = timeout
        super().__init__(self.connect_timeout, loop, 'failed to open connection to the host')
        self.host = host
        self.port = port
        self.loop = loop
        self._reader = self._writer = None

    async def get(self, request):
        """Automatically retry on failure. Raises all distinct errors when max retries exceeded.

        :param string request: request sent to the host
        :return (string, string): headers and body of the response
        """
        errors = []
        retries = 0
        while True:
            try:
                return await self._get(request)
            except HostError as e:
                errors.append(e)
                if retries >= self._READ_RETRIES:
                    if len(set(map(type, errors))) == 1:
                        raise
                    else:
                        break
                retries += 1
                await self.connect()
        raise MultipleErrors(errors)

    async def _get(self, request):
        # send the request
        self._writer.write(request)
        try:
            await self._writer.drain()
        except ConnectionError as e:
            raise HostError('connection to the host was reset') from e
        # read the response
        response = await self.read()
        if not response:
            _logger.debug('no response from the host on %s', request)
            raise NoResponseReadError('no response from the host')
        # disassemble the response to check integrity
        try:
            headers, body = response.split(b'\r\n\r\n', maxsplit=1)
        except ValueError as e:
            _logger.debug('response: \n%s', response)
            raise ResponseError('response from the host was invalid') from e
        else:
            return (headers, body)

    async def read(self):
        response = b''
        while True:
            task = self._reader.read(16384)
            # Time out if the server has not issued a response for read_timeout seconds
            if not response:
                chunk = await asyncio.wait_for(task, self.read_timeout, loop=self.loop)
            else:
                chunk = await task
            # Which means the response contains no end-of-response information
            if not chunk:
                break
            # Check if the response contains the 'Transfer-Encoding: chunked' header
            match = self._PATTERN_TE.search(response)
            if match:
                try:
                    length, content = chunk.split(b'\r\n', maxsplit=1)
                except ValueError:
                    pass
                else:
                    if int(length) > 0:
                        response += content.rstrip(b'\r\n')
                    else:
                        break
            else:
                response += chunk
                # Check if the response contains the 'Content-Length' header
                try:
                    headers, body = response.split(b'\r\n\r\n', maxsplit=1)
                except ValueError:
                    pass
                else:
                    match = self._PATTERN_CL.search(headers)
                    if match:
                        content_length = int(match.group(1))
                        if len(body) == content_length:
                            break
        return response

    async def _open_connection(self):
        if self._writer:
            await self.disconnect()
            _logger.debug('Trying to reconnect to the host')
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port, loop=self.loop)

    async def disconnect(self):
        self._writer.close()
        self._reader = self._writer = None

