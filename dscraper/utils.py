import logging
from functools import update_wrapper
import asyncio

def get_logger(name):
    logger = logging.getLogger(name)
    logger.addHandler(logging.NullHandler())
    return logger

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
