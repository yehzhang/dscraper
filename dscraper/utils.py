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
    match = _PATTERN_ST.search(raw)
    try:
        return int(match.group(1))
    except TypeError:
        pass

_PATTERN_ST = re.compile(b'HTTP/1.1 (\d+) ')

def inflate_and_decode(raw):
    dobj = zlib.decompressobj(-zlib.MAX_WBITS)
    try:
        inflated = dobj.decompress(raw)
        inflated += dobj.flush()
        return inflated.decode()
    except (zlib.error, UnicodeDecodeError) as e:
        _logger.debug('cannot decode: \n%s', raw)
        raise DecodeError('failed to decode the data from the response') from e

def parse_comments_xml(text):
    # Escape invalid chracters with their hexadecimal notations
    text = _PATTERN_ILL_XML_CHR.sub(_REPL_ILL_XML_CHR, text)
    try:
        root = et.fromstring(text)
    except et.ParseError as e:
        raise ParseError('failed to parse the XML data') from e
    if root.text == 'error':
        raise ContentError('the XML data contains a single element with "error" as content')
    # TODO split attrs
    return root

illegal_xml_chrs = [(0x00, 0x08), (0x0B, 0x0C),
                     (0x0E, 0x1F), (0x7F, 0x84),
                     (0x86, 0x9F), (0xFDD0, 0xFDDF),
                     (0xFFFE, 0xFFFF), (0x1FFFE, 0x1FFFF),
                     (0x2FFFE, 0x2FFFF), (0x3FFFE, 0x3FFFF),
                     (0x4FFFE, 0x4FFFF), (0x5FFFE, 0x5FFFF),
                     (0x6FFFE, 0x6FFFF), (0x7FFFE, 0x7FFFF),
                     (0x8FFFE, 0x8FFFF), (0x9FFFE, 0x9FFFF),
                     (0xAFFFE, 0xAFFFF), (0xBFFFE, 0xBFFFF),
                     (0xCFFFE, 0xCFFFF), (0xDFFFE, 0xDFFFF),
                     (0xEFFFE, 0xEFFFF), (0xFFFFE, 0xFFFFF),
                     (0x10FFFE, 0x10FFFF)]
illegal_ranges = [r'{}-{}'.format(chr(low), chr(high)) for low, high in illegal_xml_chrs]
_PATTERN_ILL_XML_CHR = re.compile(r'[{}]'.format(r''.join(illegal_ranges)))
_REPL_ILL_XML_CHR = lambda x: r'\x{:02X}'.format(ord(x.group(0)))
del illegal_xml_chrs, illegal_ranges

def parse_rolldate_json(text):
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise ParseError('failed to parse the JSON data') from e

def merge_xmls(xmls):
    # TODO
    pass

def strip_invalid_xml_chars(text):
    pass

def capitalize(s):
    return s[0].upper() + s[1:]

_CONNECT_RETRIES = 2

class AutoConnector:

    template = '{}'

    def __init__(self, timeout, loop, fail_message=None, retries=_CONNECT_RETRIES):
        self._timeout = timeout
        self.loop = loop
        self.retries = retries
        if fail_message:
            self.template = fail_message + ': ' + self.template

    async def connect(self):
        backoff = 0
        for tries in range(self.retries + 1):
            try:
                return await asyncio.wait_for(self._open_connection(), self._timeout, loop=self.loop)
            except asyncio.TimeoutError:
                await asyncio.sleep(backoff)
                backoff = backoff ** 2 if backoff > 0 else 1
        message = self.template.format('connection timed out')
        raise ConnectTimeout(message)

    async def disconnect(self):
        raise NotImplementedError

    async def _open_connection(self):
        raise NotImplementedError

class Retry:

    def __init__(self, connect, read, ):
        pass