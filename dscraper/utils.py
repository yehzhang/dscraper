from functools import update_wrapper
import warnings
import asyncio
import re
import xmltodict as x2d
import json

from .exceptions import ConnectTimeout, MultipleErrors, ParseError

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

def aretry(exc, exc_handler=None):
    @decorator
    def _true_decorator(coro):
        async def _f(*args, **kwargs):
            errors = []
            tries = 0
            while True:
                try:
                    return await coro(*args, **kwargs)
                except exc as e:
                    errors.append(e)
                if tries >= _RETRIES:
                    raise MultipleErrors(errors)
                tries += 1
                if _f._exc_handler:
                    try:
                        await _f._exc_handler()
                    except TypeError:
                        # pass in 'self.method' to call a class method
                        names = exc_handler.split('.')
                        if len(names) != 2 or name[0] != 'self':
                            raise ValueError('{} is not a valid function nor method'.format(repr(exc_handler)))
                        _f._exc_handler = getattr(args[0], _f._exc_handler)
                        await _f._exc_handler()

        _f._exc_handler = exc_handler

        return _f
    return _true_decorator

_RETRIES = 2

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

def parse_xml(text):
    # TODO
    #   XML string containing a single element with 'error' as content, or
    #   XML string with invalid characters
    try:
        xml = x2d.parse(text)
    except Exception as e: # TODO what exception means what?
        raise ParseError('cannot parse as XML') from e
    return xml

def parse_json(text):
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise ParseError('cannot parse as JSON') from e

def merge_xmls(xmls):
    # TODO
    pass


class AutoConnector:

    def __init__(self, timeout, loop=None):
        self._timeout = timeout
        self.loop = loop

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.disconnect()

    @aretry(ConnectTimeout)
    async def connect(self):
        try:
            return await asyncio.wait_for(self._open_connection(), self._timeout, loop=self.loop)
        except asyncio.TimeoutError as e:
            raise ConnectTimeout('connection timed out') from e

    async def disconnect(self):
        raise NotImplementedError

    async def _open_connection(self):
        raise NotImplementedError

