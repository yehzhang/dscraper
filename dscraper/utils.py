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

def parse_comments_xml(text):
    # Escape invalid XML chracters with their hexadecimal notations
    text = _PATTERN_ILL_XML_CHR.sub(_REPL_ILL_XML_CHR, text)

    try:
        root = et.fromstring(text)
    except et.ParseError:
        raise ParseError('failed to parse the XML data') from None

    # Check content
    if root.text == 'error':
        raise ContentError('the XML data contains a single element with "error" as content')

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

def deserialize_comment_attributes(root):
    for d in root.iterfind('d'):
        sattr = d.attrib.get('p')
        offset, mode, font_size, color, date, pool, user, comment_id = sattr.split(',')
        # offset = float(offset)
        # if offset % 1 == 0:
        #     offset = int(offset)
        # d._cmt_attrs = {
        #     'offset': offset,
        #     'mode': int(mode),
        #     'font_size': int(font_size),
        #     'color': int(color),
        #     'date': int(date),
        #     'pool': int(pool),
        #     'user': user,
        #     'comment_id': int(comment_id)
        # }
        d._cmt_offset = offset
        d._cmt_mode = mode
        d._cmt_font_size = font_size
        d._cmt_color = color
        d._cmt_date = int(date)
        d._cmt_pool = int(pool)
        d._cmt_user = user
        d._cmt_id = int(comment_id)
        # d._cmt_user = int(user, 16)
        # d._cmt_is_tourist = (user[0] == 'D')
    # Add indentation to the last element
    # What if there is not comment element?
    # d.tail = CommentFlow.XML_ELEM_TAIL

def parse_rolldate_json(text):
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        raise ParseError('failed to parse the JSON data') from None


class CommentFlow:
    """Data container class. Build a flow of comments from the lastest to the earliest.
    Prepend must be called after each yield of iterscrape

    TODO: add choice of alternative splitting ways in case of no splitter present
    """
    MAX_TIMESTAMP = MAX_INT
    MAX_CMT_ID = MAX_LONG
    XML_ELEM_TAIL = '\n\t'
    _ROOT_HEADERS = ('chatserver', 'chatid', 'mission', 'maxlimit', 'source',
                     'ds', 'de', 'max_count')
    _HISTORY_HEADERS = ('chatserver', 'chatid', 'mission', 'maxlimit', 'max_count')
    _LEN_HEADER = len(_HISTORY_HEADERS)

    def __init__(self, root, limit):
        self.root, self.limit = root, limit
        self.splitter = None
        self._pools = ([], [], [], []) # normal, protected, title, code
        self._flows = None

    def prepend(self, normal, protected, title, code):
        for segment, pool in zip((normal, protected, title, code), self._pools):
            if segment:
                pool.append(segment)

    def set_splitter(self, splitter):
        self.splitter = splitter

    def can_split(self):
        return bool(self.splitter)

    def get_root(self):
        return self.root

    def get_header(self, lite=False):
        """Return a list of XML elements containing metadata."""
        header = []
        for k in self._HISTORY_HEADERS if lite else self._ROOT_HEADERS:
            elem = self.root.find(k)
            if elem is None:
                elem = et.Element(k)
                elem.text = '0' if k != 'de' else str(self.MAX_TIMESTAMP)
                elem.tail = self.XML_ELEM_TAIL
                _logger.debug('metadata \"%s\" missing, using default value', k)
            header.append(elem)
        return header

    def trim(self, start, end):
        """Discard all comments not in the time range [start, end]
        Also join all segments into flows internally.
        """
        self._flows = []
        if start > end:
            return
        for flow in self._get_flows():
            ifront, irear = self.MAX_CMT_ID, 0
            for i, cmt in enumerate(flow):
                if cmt._cmt_date >= start:
                    ifront = i
                    break
            for i, cmt in enumerate(reversed(flow)):
                if cmt._cmt_date <= end:
                    irear = len(flow) - i
                    break
            self._flows.append(flow[ifront:irear])

    def components(self):
        """Return a list of headers and a list of all XML elements of comment,
        including history.
        """
        return (self.get_header(), itertools.chain(*self._get_flows()))

    def histories(self):
        """Yields one XML for each of the timestamps in the Roll Dates provided
        in iterscrape. If merge is False, XMLs are built as if they are directly
        scraped from comment.bilibili.com/dmroll,[timestamp],[cid]

        :yield (timestamp, XML):
        """
        if not self.splitter:
            raise RuntimeError('no splitter available')
        if not self._flows:
            self._flows = self._get_flows()

        header = self.get_header(True)
        root = et.Element('i')
        root[:] = header
        root.text = self.XML_ELEM_TAIL # indentation
        xml = et.ElementTree(root)

        growers = list(map(self._grow, self._flows))
        for grower in growers:
            grower.send(None)
        for date in (rd['timestamp'] for rd in self.splitter):
            root[self._LEN_HEADER:] = itertools.chain(*map(lambda x: x.send(date), growers))
            last_elem = root[-1]
            last_elem.tail = '\n' # remove indentation of the last element
            yield (date, xml)
            last_elem.tail = self.XML_ELEM_TAIL

    def _get_flows(self):
        return map(self._join, self._pools)

    def _grow(self, flow):
        cmts = None
        i, length = 0, len(flow)
        while i < length:
            date = yield cmts
            for i in range(i, length):
                if flow[i]._cmt_date > date:
                    cmts = flow[max(i - self.limit, 0):i]
                    break
        while True:
            yield flow

    @staticmethod
    def _join(segments):
        """Join segments
        """
        flow = []
        horizon = 0
        for segment in reversed(segments):
            for i, cmt in enumerate(segment):
                if cmt._cmt_id > horizon:
                    horizon = segment[-1]._cmt_id
                    flow.extend(segment[i:])
                    break
        return flow

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
TIME_CONFIG_US = (0, 1, 18, 22, timezone('America/Los_Angeles'))

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
    # TODO choose time config from the host's location
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
        except TypeError:
            raise TypeError('hour must be integer') from None
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

