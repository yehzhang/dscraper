import logging

_logger = logging.getLogger(__name__)


class DscraperError(Exception):
    """A general error that Dscraper has nothing to do with it.
    Manual retrying is not recommended until the problem causing this error is
    solved, because Dscraper has retried automatically.
    """
    damage = 1
    level = logging.INFO

    def __init__(self, message=None, logging_level=True):
        if message is None:
            super().__init__()
        else:
            super().__init__(message)
        if logging_level is not True:
            self.level = logging_level

    def results_in(self, consequence):
        self.args = ('{}: {}'.format(consequence, self.args[0]), )


class HostError(DscraperError, OSError):
    """An error occured while trying to communicate with the host.
    This error is most likely caused by the host. Some frequent causes are that
    your IP was blocked by the host, and that your Internet connection was lost.
    The only solution is to wait.
    """
    damage = 40


class ConnectTimeout(HostError):
    """All attempts to connect to the host timed out."""
    level = logging.WARNING

class ReadTimeout(HostError):
    """Attempt to read from the host timed out."""

class ResponseError(HostError):
    """The response from the host was invalid."""


class NoResponseReadError(ResponseError):
    """There is no response from the host.
    Basically it is caused by opening a 404 page, or keeping the host waiting too long,
    so that the connection was closed. This exception occurs frequently.
    """
    damage = 10


class DataError(DscraperError, ValueError):
    """The data read from the response was invalid.
    This error is most likely caused by the host, but even the host would not solve it.
    Sometimes the host sends mal-formatted files, especially when you scrape those generated
    years ago, when the host's server was rather buggy at that time.
    There is no solution.
    """
    damage = 30


class DecodeError(DataError):
    """The byte array cannot be decoded."""
    level = logging.WARNING


class ParseError(DataError):
    """The string cannot be parsed as XML or JSON."""


class ContentError(DataError):
    """The recieved XML data contains a single element with "error" as content."""
    damage = 5


class PageNotFound(DscraperError):
    """The given uri ended in a 404 page, which is very likely to happen."""
    damage = 0


class MultipleErrors(DscraperError):
    """The container of multiple errors."""

    def __init__(self, errors):
        super().__init__('')
        errors = set((type(e), e.args[0], e.damage) for e in errors)
        self.types, messages, damages = zip(*errors)
        message = '{} distinct error(s) occured: \n{}'.format(len(errors), ';\n'.join(messages))
        self.args = (message,)
        self.damage = max(damages)


class NoMoreItems(DscraperError):
    """A replacement of StopIteration in coroutines. Internal use only."""


class Scavenger:
    """Handles and logs all exceptions."""
    _MAX_HEALTH = 120
    _REGEN = 12
    _UNEXPECTED_DAMAGE = 100

    def __init__(self):
        self.dead = False
        self._health = self._max_health = self._MAX_HEALTH
        self._recorders = 1
        self._cnt_success = 0
        self._failures = []

    def set_recorders(self, num):
        _logger.debug('set %d recorders', num)
        if num < 0:
            raise ValueError('cannot set \'{}\' recorders'.format(num))
        self._health = self._health / self._recorders * num
        self._max_health = self._MAX_HEALTH * num
        self._recorders = num

    def success(self):
        self._health = min(self._health + self._REGEN, self._max_health)
        self._cnt_success += 1

    def failure(self, worker, e=None):
        # TODO log worker type, change cid to aid or sth in logging
        cid = str(worker.item) if worker.item else '\'not started yet\''
        if e is None:
            _logger.exception('Unexpected exception occured during scraping cid %s', cid)
            self._health -= self._UNEXPECTED_DAMAGE
        else:
            message = '{} at CID {}'.format(self.capitalize(e.args[0]), cid)
            if e.__cause__:
                message += ': ' + e.__cause__
            _logger.log(e.level, message)
            self._health -= e.damage
        if self._health <= 0:
            if not self.dead:
                _logger.critical('Too many exceptions triggered. The scraper is about to stop')
            self.dead = True
        if isinstance(e, PageNotFound):
            self._cnt_success += 1
        else:
            self._failures.append(cid)
        _logger.debug('health: %d / %d, recorders: %d', self._health,
                      self._max_health, self._recorders)

    def is_dead(self):
        return self.dead

    def get_failures(self):
        return self._failures

    def get_success_count(self):
        return self._cnt_success

    @staticmethod
    def capitalize(s):
        return s[0].upper() + s[1:]
