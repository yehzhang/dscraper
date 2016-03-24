from functools import update_wrapper
from collections import deque
from pytz import timezone
from datetime import datetime
import asyncio
import re
import xml.etree.ElementTree as et
import json
import logging
import itertools

from .exceptions import ParseError, ContentError, ConnectTimeout

_logger = logging.getLogger(__name__)

MAX_INT = 2147483647
MAX_LONG = 9223372036854775807


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


def validate_id(target):
    try:
        if target <= 0:
            raise ValueError(
                'invalid target from input: a positive integer is required, not \'{}\''
                .format(target))
    except TypeError:
        raise TypeError('invalid target from input: an integer is required, not \'{}\''
                        .format(type(target).__name__)) from None


def escape_invalid_xml_chars(text):
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


def parse_comments_xml(text):
    try:
        root = et.fromstring(text)
    except et.ParseError:
        raise ParseError('failed to parse the XML data') from None

    # Check content
    if root.text == 'error' or len(root) == 0:
        raise ContentError('content of the XML document is invalid')

    # deserialize attributes
    for d in root.iterfind('d'):
        offset, mode, font_size, color, date, pool, user, cmt_id = d.attrib['p'].split(',')
        d.attrib = {
            'offset': offset,
            'mode': mode,
            'font_size': font_size,
            'color': color,
            'date': int(date),
            'pool': int(pool),
            'user': user,
            'id': int(cmt_id),
            'p': d.attrib['p']
        }

    return root


def parse_rolldate_json(text):
    try:
        rd = json.loads(text)
    except json.JSONDecodeError:
        raise ParseError('failed to parse the JSON data') from None

    try:
        return [int(date['timestamp']) for date in rd]
    except KeyError:
        raise ContentError('content of the Roll Date is invalid') from None


class CommentFlow:
    """Data container class. Keeps all comments in a flow and cuts a piece from it for export.
    """
    MAX_TIMESTAMP = MAX_INT
    MAX_CMT_ID = MAX_LONG
    _ROOT_HEADERS = ('chatserver', 'chatid', 'mission', 'maxlimit', 'source')
    _HISTORY_HEADERS = ('chatserver', 'chatid', 'mission', 'maxlimit')

    def __init__(self, latest, histories, flows, roll_dates, limit):
        self.latest = latest  # root element with children referenced in the flows.
        self._histories_roots = histories
        self.flows = flows
        self._splitter = roll_dates
        self.limit = limit

    def can_split(self):
        return bool(self._splitter)

    def has_history(self):
        return bool(self._histories_roots)

    def get_latest(self):
        return self.latest

    def get_all_comments(self):
        """Return a list of all comment Elements.
        Mostly called by database exporters, which require pure data and do not
        care about what form it is.

        :return [Element]:
        """
        return itertools.chain(*self.flows)

    def get_histories(self):
        """Yields a list of Elements included in a comment document for each of
        the timestamps in the Roll Dates.
        Mostly called by file exporters, which require comments splitted by Roll
        Dates.

        :yield (timestamp, [Elements]):
        """
        if not self._histories_roots:
            raise RuntimeError('no history available')
        elif not self._splitter:
            raise RuntimeError('no splitter available')

        header = self._get_header(self._HISTORY_HEADERS)
        growers = [self._grow(flow, self.limit) for flow in self.flows]
        for grower in growers:
            grower.send(None)

        for date in self._splitter:
            root = self._histories_roots.get(date, None)
            _logger.debug('histories, date: %d, %s', date,
                          'generated' if root is None else 'origin')
            if root is None:
                root = itertools.chain(header, *map(lambda x: x.send(date), growers))
            yield (date, root)

    def get_document(self):
        """Return a list of metadata Elements and of all comment Elements.
        Mostly called by file exporters when there is history but no splitter.

        :return [Elements]:
        """
        header = self._get_header(self._ROOT_HEADERS)
        return itertools.chain(header, *self.flows)

    def _get_header(self, header_type):
        """Return a list of metadata Elements.

        :return [Element]:
        """
        header = []
        for k in header_type:
            elem = self.latest.find(k)
            if elem is not None:
                header.append(elem)
            else:
                _logger.debug('metadata \"%s\" missing', k)
        return header

    @staticmethod
    def _grow(flow, limit):
        """Dates sended must be in ascending order."""
        date = yield
        i, length = 0, len(flow)
        while i < length:
            if flow[i].attrib['date'] > date:
                date = yield flow[max(i - limit, 0):i]
            else:
                i += 1
        while True:
            yield flow[-limit:]


class AutoConnector:

    template = '{}'

    def __init__(self, timeout, fail_message=None, retries=2, *, loop):
        self._timeout = timeout
        self.loop = loop
        self.retries = retries
        if fail_message:
            self.template = fail_message + ': ' + self.template

    async def connect(self):
        for tries in range(self.retries + 1):
            try:
                return await asyncio.wait_for(self._open_connection(), self._timeout,
                                              loop=self.loop)
            except asyncio.TimeoutError:
                await asyncio.sleep(tries ** 2)
        message = self.template.format('connection timed out')
        raise ConnectTimeout(message)

    async def disconnect(self):
        pass

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
        return max(self._value, 0)

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

TIME_CONFIG_CN = (0, 1, 18, 22.5, timezone('Asia/Shanghai'))
TIME_CONFIG_US = (0, 1, 18, 22.5, timezone('America/Los_Angeles'))


class FrequencyController:
    """
    Limits the frequency of coroutines running across.

    :param tuple time_config: a 5-tuple of the configuration of the controller's behavior
        float interval: duration of waiting in common hours
        float busy_interval: duration of waiting in rush hours
        float start: beginning of the rush hour
        float end: end of the rush hour
        tzinfo timezone: time zone where the host is
    """
    # TODO choose time config from the host's geolocation

    def __init__(self, time_config=TIME_CONFIG_CN, *, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.interval, self.busy_interval, start, end, self.tz = time_config
        if not (0 <= start < 24 and 0 <= end < 24):
            raise ValueError('hours not in range [0, 24)')
        if start < end:
            self._is_rush_hour = lambda x: start <= x < end
        else:
            self._is_rush_hour = lambda x: 0 <= x < end or start <= x < 24
        self._latch = asyncio.Semaphore(loop=loop)
        self._blocking = True

    async def wait(self):
        """Controls frequency."""
        if self._blocking:
            now = datetime.now(tz=self.tz)
            hour = now.hour + now.minute / 60 + now.second / 3600
            interval = self.busy_interval if self._is_rush_hour(hour) else self.interval
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

    async def wait(self):
        pass
