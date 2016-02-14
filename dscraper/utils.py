from functools import update_wrapper
from collections import deque
from pytz import timezone
from datetime import datetime
import asyncio
import re
import xml.etree.ElementTree as et
import json
import zlib
import logging

from .exceptions import ParseError, DecodeError, ContentError, ConnectTimeout

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
        sig = SIGNATURE.format(name=f.__name__,
                               args=sa + ', ' + skwa if sa and skwa else sa + skwa)
        sin = FORMAT_IN.format(indent=INDENT * trace._depth, tid=tid, signature=sig)
        print(sin)

        trace._depth += 1
        try:
            result = f(*args, **kwargs)
            sout = FORMAT_OUT.format(indent=INDENT * (trace._depth - 1), tid=tid,
                                     result=repr(result))
            print(sout)
            return result
        finally:
            trace._depth -= 1

    return _f

trace._traced = 0
trace._depth = 0

SIGNATURE = '{name}({args})'
INDENT = '   '
FORMAT_IN = '{indent}{signature} -> #{tid}'
FORMAT_OUT = '{indent}{result} <- #{tid}'

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

_PATTERN_ST = re.compile(b'HTTP/1.1 (\\d+) ')

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
    try:
        root = et.fromstring(text)
    except et.ParseError as e:
        raise ParseError('failed to parse the XML data') from e
    if root.text == 'error':
        raise ContentError('the XML data contains a single element with "error" as content')
    return root

def strip_invalid_xml_chars(text):
    """Escape invalid XML chracters with their hexadecimal notations."""
    return _PATTERN_ILL_XML_CHR.sub(_REPL_ILL_XML_CHR, text)

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

def deserialize_comment_attributes(root):
    for d in root.iterfind('d'):
        sattr = d.attrib.get('p')
        offset, mode, font_size, color, date, pool_id, user_id, comment_id = sattr.split(',')
        offset = float(offset)
        if offset % 1 == 0:
            offset = int(offset)
        d.attrib = {
            'offset': offset,
            'mode': int(mode),
            'font_size': int(font_size),
            'color': int(color),
            'date': int(date),
            'pool_id': int(pool_id),
            'user_id': user_id,
            'comment_id': int(comment_id)
        }

def serialize_comment_attributes(root):
    for d in root.iterfind('d'):
        sattr = ','.join(map(lambda x: str(d.attrib[x]), SATTRIBUTE))
        d.attrib = {'p': sattr}

SATTRIBUTE = ('offset', 'mode', 'font_size', 'color', 'date', 'pool_id', 'user_id', 'comment_id')

def parse_rolldate_json(text):
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise ParseError('failed to parse the JSON data') from e

def split_xml(xml, splitter):
    if splitter is None:
        return xml
    # TODO get maxlimit and form the current xml, then form history xmls.
    # remember to attach headers to all files
    pass

def merge_xmls(xmls):
    # TODO
    pass

_CONNECT_RETRIES = 2

class AutoConnector:

    template = '{}'

    def __init__(self, timeout, fail_message=None, retries=_CONNECT_RETRIES, *, loop):
        self._timeout = timeout
        self.loop = loop
        self.retries = retries
        if fail_message:
            self.template = fail_message + ': ' + self.template

    async def connect(self):
        for tries in range(self.retries + 1):
            try:
                return await asyncio.wait_for(self._open_connection(), self._timeout, loop=self.loop)
            except asyncio.TimeoutError:
                await asyncio.sleep(tries ** 2)
        message = self.template.format('connection timed out')
        raise ConnectTimeout(message)

    async def disconnect(self):
        raise NotImplementedError

    async def _open_connection(self):
        raise NotImplementedError

class CountLatch:
    """A CountLatch implementation.

    A semaphore manages an internal counter which is incremented by each count() call
    and decremented by each count_down() call. When wait() finds that the counter is
    greater than zero, it blocks, waiting until some other thread calls count_down().

    :param int value: initial value for the internal counter; it defaults to 1.
        If the value given is less than 0, ValueError is raised.
    """

    def __init__(self, value=0, *, loop=None):
        if value < 0:
            raise ValueError("initial value must be 0 or greater")
        self._value = value
        self._waiters = deque()
        self._loop = loop if loop else asyncio.get_event_loop()

    def locked(self):
        """Returns True if semaphore can not be acquired immediately."""
        return self._value > 0

    def count(self, num=1):
        """Increase a count."""
        if num <= 0:
            raise ValueError('cannot increase \'{}\' counts'.format(num))
        self._value += num

    def count_down(self, num=1):
        """Decrease a count, incrementing the internal counter by one.
        When it was greater than zero on entry and other coroutines are waiting for
        it to become smaller than or equal to zero again, wake up those coroutines.
        """
        if num <= 0:
            raise ValueError('cannot decrease \'{}\' counts'.format(num))
        self._value -= num

        released = False
        if self._value <= 0:
            for waiter in self._waiters:
                if not waiter.done():
                    waiter.set_result(True)
                    released = True
        return released

    def __len__(self):
        return min(self._value, 0)

    async def wait(self):
        """Wait until the internal counter is not larger than zero."""
        if self._value <= 0:
            return True

        fut = asyncio.Future(loop=self._loop)
        self._waiters.append(fut)
        try:
            await fut
            return True
        finally:
            self._waiters.remove(fut)

class Sluice:
    """
    An Sluice manages a flag that can be set to true with the set() method and
    reset to false with the clear() method. The wait() method blocks until the
    flag is true or the leak() method is called. The flag is initially false.
    """
    def __init__(self, *, loop=None):
        self._waiters = deque()
        self._loop = loop or asyncio.get_event_loop()
        self._value = False

    def leak(self):
        """Awakes all coroutines waiting for it. Does not set the internal flag.
        """
        if not self._value:
            for fut in self._waiters:
                if not fut.done():
                    fut.set_result(True)

    def set(self):
        """Set the internal flag to true. All coroutines waiting for it to
        become true are awakened. Coroutine that call wait() once the flag is
        true will not block at all.
        """
        self.leak()
        self._value = True

    def is_set(self):
        """Return True if and only if the internal flag is true."""
        return self._value

    def clear(self):
        """Reset the internal flag to false. Subsequently, coroutines calling
        wait() will block until set() is called to set the internal flag
        to true again or the leak() method is called."""
        self._value = False

    async def wait(self):
        """Block until the internal flag is true.

        If the internal flag is true on entry, return True immediately.
        Otherwise, block until another coroutine calls set() to set the flag
        to true or leak(), then return True.
        """
        if self._value:
            return True

        fut = asyncio.Future(loop=self._loop)
        self._waiters.append(fut)
        try:
            await fut
            return True
        finally:
            self._waiters.remove(fut)

TIME_CONFIG_CN = (0, 1, 18, 22, timezone('Asia/Shanghai'))

class FrequencyController:
    """
    Limits the frequency of coroutines running across.

    :param tuple time_config: a 5-tuple of the configuration of the controller's behavior
        number interval: duration of waiting in common hours
        number busy_interval: duration of waiting in rush hours
        int start: beginning of the rush hour
        int end: ending of the rush hour
        tzinfo timezone: time zone where the host is
    """
    def __init__(self, time_config=TIME_CONFIG_CN, *, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.interval, self.busy_interval, start, end, self.tz = time_config
        if start > 23 or end < 0:
            raise ValueError('hour must be in [0, 23]')
        try:
            if start < end:
                self.rush_hours = set(range(start, end))
            else:
                self.rush_hours = set(range(start, 24)) | set(range(0, end + 1))
        except TypeError as e:
            raise TypeError('hour must be integer') from e
        self._latch = asyncio.Semaphore(loop=loop)
        self._blocking = True

    async def wait(self):
        """Controls frequency."""
        if self._blocking:
            hour = datetime.now(tz=self.tz).hour
            interval = self.busy_interval if hour in self.rush_hours else self.interval
            if interval > 0:
                _logger.debug('Controller blocking')
                await self._latch.acquire()
                self.loop.call_later(interval, self._latch.release)
                return True
        return False

    def free(self):
        """Stop blocking coroutines."""
        if self._blocking:
            self._latch.release()
            self._blocking = False

    def shut(self):
        """Start blocking coroutines."""
        if not self._blocking:
            self.loop.run_until_complete(self._latch.acquire())
            self._blocking = True

class NullController:

    def __init__(self, *args, **kwargs):
        pass

    async def wait(self):
        pass

