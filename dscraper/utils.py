from functools import update_wrapper
import warnings
import asyncio
import re
import xml.etree.ElementTree as et
import json
import zlib
import logging

from .exceptions import ConnectTimeout, MultipleErrors, ParseError

_logger = logging.getLogger(__name__)


def decorator(d):
    return lambda f: update_wrapper(d(f), f)
decorator = decorator(decorator)

@decorator
def trace(f):
    def _f(*args, **kwargs):
        trace._traced += 1
        tid = trace._traced

        sa = ', '.join(map(repr, args))
        skwa = ', '.join('{}={}'.format(k, repr(v)) for k, v in kwargs.items())
        sig = signature.format(name=f.__name__,
                               args=sa + ', ' + skwa if sa and skwa else sa + skwa)
        sin = format_in.format(indent=indent * trace._depth, tid=tid, signature=sig)
        print(sin)

        trace._depth += 1
        try:
            result = f(*args, **kwargs)
            sout = format_out.format(indent=indent * (trace._depth - 1), tid=tid,
                                     result=repr(result))
            print(sout)
            return result
        finally:
            trace._depth -= 1

    trace._traced = 0
    trace._depth = 0

    return _f

signature = '{name}({args})'
indent = '   '
format_in = '{indent}{signature} -> #{tid}'
format_out = '{indent}{result} <- #{tid}'

@decorator
def alock(coro):
    async def _coro(*args, **kwargs):
        async with lock:
            return await coro(*args, **kwargs)
    lock = asyncio.Lock()
    return _coro


def get_headers_text(headers):
    return ''.join('{}:{}\r\n'.format(k, v) for k, v in headers.items())

def get_status_code(raw):
    match = re.search(b'HTTP/1.1 (\d+) ', raw)
    if match:
        return int(match.group(1))

def is_response_complete(raw):
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

def inflate_and_decode(raw):
    dobj = zlib.decompressobj(-zlib.MAX_WBITS)
    try:
        inflated = dobj.decompress(raw)
        inflated += dobj.flush()
        return inflated.decode()
    except (zlib.error, UnicodeDecodeError) as e:
        _logger.debug('cannot decode: \n%s', raw)
        raise DecodeError('Failed to decode the data from the response') from e

def parse_xml(text):
    # TODO XML string with invalid characters
    try:
        root = et.fromstring(text)
    except et.ParseError as e: # TODO what exception means what?
        raise ParseError('Failed to parse the XML data') from e
    if root.text == 'error':
        raise ContentError('The XML data contains a single element with "error" as content')
    return root

def parse_json(text):
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise ParseError('Failed to parse the JSON data') from e

def merge_xmls(xmls):
    # TODO
    pass

def get_all_cids(cid_iters):
    for cid_iter in cid_iters:
        try:
            for raw_cid in cid_iter:
                yield raw_cid
        except TypeError:
            yield cid_iter

def cid_checker(cid_iter):
    for cid in cid_iter:
        try:
            cid = int(cid)
        except TypeError:
            raise InvalidCid('Invalid cid from input: an integer is required, not \'{}\''.format(type(cid).__name__))
        if cid <= 0:
            raise InvalidCid('Invalid cid from input: a positive integer is required')
        yield cid


class AutoConnector:

    def __init__(self, timeout, loop=None, fail_result=None):
        self._timeout = timeout
        self.loop = loop
        self.fail_result = fail_result

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.disconnect()

    async def connect(self):
        for tries in range(_CONNECT_RETRIES + 1):
            try:
                return await asyncio.wait_for(self._open_connection(), self._timeout, loop=self.loop)
            except asyncio.TimeoutError:
                pass
        message = 'Connection timed out'
        if self.fail_result:
            message = self.fail_result + ': ' + message
        raise ConnectTimeout(message)

    async def disconnect(self):
        raise NotImplementedError

    async def _open_connection(self):
        raise NotImplementedError

_CONNECT_RETRIES = 2
