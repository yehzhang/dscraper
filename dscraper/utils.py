from functools import update_wrapper
from abc import ABCMeta, abstractmethod
import warnings
import asyncio
import re

def decorator(d):
    return lambda f: update_wrapper(d(f), f)
decorator = decorator(decorator)

@decorator
def alock(coro):
    if not asyncio.iscoroutinefunction(coro):
        raise TypeError('not a coroutine function')
    # @asyncio.coroutine
    async def _coro(*args, **kwargs):
        # not good
        # raise RuntimeError('coroutine already running')

        # not working
        # if _coro._locking:
        #     asyncio.ensure_future(_coro(*args, **kwargs))
        #     return

        # TODO not graceful
        while _coro._locking:
            await asyncio.sleep(0.1)

        _coro._locking = True
        try:
            return await coro(*args, **kwargs)
        finally:
            _coro._locking = False

    _coro._locking = False

    return _coro

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
    signature = '{name}({args})'
    indent = '   '
    format_in = '{indent}{signature} -> #{tid}'
    format_out = '{indent}{result} <- #{tid}'

    return _f


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


_RETRIES = 2

class AutoConnector(metaclass=ABCMeta):

    def __init__(self, timeout):
        self._connected = False
        self._timeout = timeout

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.disconnect()

    async def connect(self):
        if self._connected:
            warnings.warn('Connector is connected when connect() is called')
            self.disconnect()
        # if connection timed out, retry
        for tries in range(1, _RETRIES + 1):
            try:
                await asyncio.wait_for(self.on_connect(), self._timeout)
            except asyncio.TimeoutError:
                pass
            else:
                self.connected = True
                return
        raise ConnectTimeout('max retries exceeded')

    def disconnect(self):
        if self._connected:
            self.on_disconnect()
            self._connected = False
        else:
            warnings.warn('Connector is not connected when disconnect() is called')

    @abstractmethod
    async def on_connect(self):
        pass

    @abstractmethod
    def on_disconnect(self):
        pass