__all__ = ('Fetcher', )

import asyncio
import zlib
import re

from . import utils
from .exceptions import ConnectionError, ConnectTimeout, NoResponseError, DecodeError

_logger = utils.get_logger(__name__)

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
_RETRIES = 3

class Fetcher:

    def __init__(self, loop=None):
        self.connected = False
        self.loop = loop
        self.headers = _DEFAULT_HEADERS
        self.template = _REQUEST_TEMPLATE.format(headers=
            ''.join('{}:{}\r\n'.format(k, v) for k, v in self.headers.items()))
        self.decompressor = zlib.decompressobj(-zlib.MAX_WBITS)

    async def connect(self):
        self.disconnect()
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

    async def fetch(self, uri):
        # raise ConnectionError (& ConnectTimeout), DecodeError
        # send the request and read the raw response body
        request = self.template.format(uri=uri).encode('ascii')
        for tries in range(1, _RETRIES + 1):
            self.writer.write(request)
            try:
                await self.writer.drain()
                raw = await self._read()
            except ConnectionResetError:
                _logger.info('Connection to %s was reset', uri)
            except NoResponseError:
                _logger.info('No response was recieved from %s', uri)
            else:
                # produce output
                if not raw:
                    return None
                try:
                    return self.decompressor.decompress(raw).decode()
                except (zlib.error, UnicodeDecodeError) as e:
                    _logger.warning('Failed to decode the raw content from %s for %s', uri, e)
                    raise DecodeError('cannot decode the raw content')
            await self.connect()
        _logger.warning('Failed to read from %s after %d tries', uri, _RETRIES)
        raise ConnectionError('cannot read from %s' % uri)

    def disconnect(self):
        if self.connected:
            self.writer.close()
            self.connected = False

    async def _read(self):
        body = None

        # TODO no data from host time-out / read time-out
        response = b''
        while True:
            chunk = await self.reader.read(1024)
            if not chunk:
                break
            response += chunk

            # determine the length of response by looking for Content-Length
            parts = response.split(b'\r\n\r\n', maxsplit=1)
            if len(parts) is not 2:
                continue
            headers, body = parts
            match = re.search(b'Content-Length: (\d+)\r\n', headers)
            # if Content-Length is found, read bytes of the same length only,
            # which are supposed to be the body of response
            content_length = int(match.group(1))
            while len(body) < content_length:
                chunk = await self.reader.read(16384)
                if not chunk:
                    break
                body += chunk
            break
        if not response:
            raise NoResponseError
        if not body:
            headers, body = response.split(b'\r\n\r\n', maxsplit=1)

        if _get_status_code(headers) is 404:
            _logger.info('%s is a 404 page', uri)
            return None
        return body



def _get_status_code(raw):
    match = re.search(b'HTTP/1.1 (\d+) ', raw)
    if match:
        return int(match.group(1))
    return None