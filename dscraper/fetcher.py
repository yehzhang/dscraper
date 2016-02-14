import logging
import asyncio
import re
from collections import defaultdict

from .utils import FrequencyController, NullController, AutoConnector, get_headers_text, get_status_code, inflate_and_decode, strip_invalid_xml_chars
from .exceptions import HostError, ConnectTimeout, ResponseError, MultipleErrors, NoResponseReadError, PageNotFound
from . import __version__, _DEBUGGING

_logger = logging.getLogger(__name__)


_HOST_CID = 'comment.bilibili.com'
_HOST_AID = 'bilibili.com'
_PORT = 80

class BaseFetcher:
    """High-level utility class that fetches data from bilibili.com.

    """
    _DEFAULT_HEADERS = {
        'User-Agent': 'dscraper/' + __version__
    }
    # TODO: switch to backup headers if necessary. when neccessary?
    _BACKUP_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/600.7.12 (KHTML, like Gecko) Version/8.0.7 Safari/600.7.12',
        'Referer': 'http://www.baidu.com/',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive'
    }

    controllers = defaultdict(FrequencyController)

    def __init__(self, host, port=_PORT, headers=None, *, loop):
        if not headers:
            headers = dict(self._DEFAULT_HEADERS)
        headers['Host'] = host
        self._controller = self.controllers[host] if not _DEBUGGING else NullController()
        self._session = Session(host, port, headers, loop=loop)
        # Export the methods
        self.open = self._session.connect
        self.close = self._session.disconnect
        self._set_headers = self._session.set_headers

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def get(self, uri):
        """Fetch the content.
        Do not await this coroutine while it is already awaited somewhere else.
        Instead create multiple Fetcher objects.

        :param string uri: the URI to fetch content from
        :raise: HostError, DecodeError, PageNotFound
        """
        await self._controller.wait()
        # try to get the response
        try:
            return await self._session.get(uri)
        except (ConnectTimeout, MultipleErrors) as e:
            raise HostError('failed to read from the host') from e
        except AttributeError as e:
            raise RuntimeError('fetcher is not opened yet') from e

class CIDFetcher(BaseFetcher):

    def __init__(self, *, loop):
        super().__init__(_HOST_CID, loop=loop)

    async def get_comments(self, cid, timestamp=0):
        if timestamp == 0:
            uri = '/{}.xml'.format(cid)
        else:
            uri = '/dmroll,{},{}'.format(timestamp, cid)
        text = await self.get(uri)
        return strip_invalid_xml_chars(text)

    async def get_rolldate(self, cid):
        uri = '/rolldate,{}'.format(cid)
        return await self.get(uri)

class MetaCIDFetcher(BaseFetcher):

    def __init__(self, *, loop):
        super().__init__(_HOST_AID, loop=loop)

    async def get_cid(self, aid):
        """Get the Chat ID of the given AV ID. Alternatively uses the APIs
        of bilibili and bilibilijj, and directly scrapes the website as fallback.
        """
        # pattern = r"EmbedPlayer('player',
        #                         "http://static.hdslb.com/play.swf",
        #                         "cid=3752723&aid=2398023");"
        # url = 'http://www.bilibili.com/video/av10000/'
        raise NotImplementedError

    async def get_aid(self, cid):
        """Get the AV ID of the given Chat ID by binary search and brute-force."""
        raise NotImplementedError
        # r = requests.get("http://www.bilibilijj.com/Api/AvToCid/2398023/1")

_DEFAULT_TIMEOUT = (3, 14)

class Session(AutoConnector):
    """Socket-level interface dealing with crude HTTP stuff. Made as a substitution
    of aiohttp, which automatically inflates the responses but sometimes fails
    in the ones containing XML data for unknown reason.
    """
    _REQUEST_TEMPLATE = 'GET {{uri}} HTTP/1.1\r\n{headers}\r\n'
    _READ_RETRIES = 2
    _PATTERN_TE = re.compile(b'Transfer-Encoding: chunked\r\n')
    _PATTERN_CL = re.compile(b'Content-Length: (\\d+)\r\n')
    _PATTERN_TE_BEGIN = re.compile(b'^([0-9A-Fa-f]+)\r\n')
    _PATTERN_TE_END = re.compile(b'\r\n([0-9A-Fa-f]+)(?:\r\n)+$')
    _LINE_BREAK = b'\r\n'
    _DOUBLE_BREAK = b'\r\n\r\n'

    def __init__(self, host, port, headers, timeout=_DEFAULT_TIMEOUT, *, loop):
        self.connect_timeout, self.read_timeout = timeout
        super().__init__(self.connect_timeout, 'failed to open connection to the host', loop=loop)
        self.host = host
        self.port = port
        self.set_headers(headers)
        self._reader = self._writer = None

    def set_headers(self, headers):
        self._template = self._REQUEST_TEMPLATE.format(headers=get_headers_text(headers))

    async def get(self, uri):
        """Retries on failure. Raises all distinct errors when max retries exceeded.

        :param string uri: URI to request from
        :return string: decoded body of the response
        """
        request = self._template.format(uri=uri).encode('ascii')
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
                await asyncio.sleep(retries ** 2)
                await self.connect()
                retries += 1
        raise MultipleErrors(errors)

    async def _get(self, request):
        # send the request
        self._writer.write(request)
        try:
            await self._writer.drain()
        except ConnectionError as e:
            raise HostError('connection to the host was reset') from e
        # read the response
        response = await self._read()
        if not response:
            _logger.debug('no response from the host on %s', request)
            raise NoResponseReadError('no response from the host')
        # disassemble the response to check integrity
        try:
            headers, body = response.split(self._DOUBLE_BREAK, 1)
        except ValueError as e:
            _logger.debug('response: \n%s', response)
            raise ResponseError('response from the host was invalid') from e
        # check the status code
        if get_status_code(headers) == 404:
            raise PageNotFound('fetching a 404 page')
        # inflate and decode the body
        return inflate_and_decode(body)

    async def _read(self):
        response = b''
        content_length = is_chunked = False

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

            if is_chunked:
                # Following chunks contain length at the beginning normally
                try:
                    _, length, content = self._PATTERN_TE_BEGIN.split(chunk, 1)
                    length = int(length, 16)
                    if length == 0:
                        break
                    response += content
                except ValueError:
                    # Except that the second chunk, which is supposed to be integral
                    # to the first one, may contain length 0 at the end
                    try:
                        content, length, _ = self._PATTERN_TE_END.split(chunk, 1)
                        length = int(length, 16)
                    except ValueError as e:
                        raise ResponseError('response from the host was invalid') from e
                    response += content
                    if length == 0:
                        break

            # Check if the response contains the 'Transfer-Encoding: chunked' header
            elif content_length is False and self._PATTERN_TE.search(chunk):
                is_chunked = True
                chunk = chunk.rstrip(self._LINE_BREAK)
                # The first chunk contains length 0 at the end
                try:
                    content, length, _ = self._PATTERN_TE_END.split(chunk, 1)
                    length = int(length, 16)
                    response += content
                    if length == 0:
                        break
                except ValueError as e:
                    # Or no length, usually
                    response += chunk

            # Check if the response contains the 'Content-Length' header
            else:
                response += chunk
                match = self._PATTERN_CL.search(response)
                if match:
                    content_length = int(match.group(1))
                    try:
                        headers, body = response.split(self._DOUBLE_BREAK, 1)
                    except ValueError:
                        pass
                    else:
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
