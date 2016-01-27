__all__ = ('DscraperError', 'HostError', 'ConnectTimeout',
           'ResponseError', 'DataError', 'DecodeError', 'ParseError',
           'InvalidCid', 'MultipleErrors', 'NoResponseReadError', 'PageNotFound')

from traceback import format_exception
from random import shuffle
import logging

_logger = logging.getLogger(__name__)

class DscraperError(Exception):
    """A general error that Dscraper has nothing to do with it.
    Manual retrying is not recommended until the problem causing this error is
    solved, because Dscraper has retried automatically.
    """
    damage = 0 # too many exceptions triggered will kill the scraper
    level = logging.INFO

    def __init__(self, message=None, logging_level=True):
        if message:
            super().__init__(message)
        else:
            super().__init__()
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

class ResponseError(HostError):
    """The response from the host was invalid."""

class NoResponseReadError(ResponseError):
    """There is no response from the host.
    Basically it is caused by opening a 404 page, or keeping the host waiting too long,
    so that the connection was closed. This exception occurs frequently."""
    damage = 10

class PageNotFound(ResponseError):
    """The given uri ended in a 404 page.
    This exception is so common that we don't care at all."""
    damage = 0

class DataError(DscraperError, ValueError):
    """The data read from the response was invalid.
    This error is most likely caused by the host, but even the host would not solve it.
    Sometimes the host sends mal-formatted comments files, especially those generated
    years ago, when the host's server was rather unstable.
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
    damage = 0

class InvalidCid(DscraperError, ValueError, TypeError):
    """The worker was fed with an invalid cid to work with."""
    damage = 24
    level = logging.WARNING

class MultipleErrors(DscraperError):
    """The container of multiple errors."""
    def __init__(self, errors):
        super().__init__()
        extract_info = lambda e: format_exception(e, e, None)[-1].rstrip('\n')
        message = '{} error(s) occured: \n{}'.format(len(errors),
                                                   ', \n'.join(map(extract_info, errors)))
        self.args = (message,)
        self.damage = max(errors, key=lambda e: e.damage)


class Watcher:

    def __init__(self, log=True):
        self.log = log
        self.dead = False
        self.register([])

    def register(self, workers):
        num_workers = max(len(workers), 1)
        num_recorders = min(num_workers, 3)
        shuffle(workers)
        self.recorders = frozenset(workers[:num_recorders])
        self.health = self._max_health = _MAX_HEALTH * num_recorders
        self.regen = round(_MAX_HEALTH * _REGEN_SPEED * (num_recorders / num_workers))

    def heal(self):
        self.health = min(self.health + self.regen, self._max_health)

    def damage(self, e, worker):
        if self.log:
            message = '{} at cid {}'.format(e.args[0], worker.cid)
            if e.__cause__:
                message += ': ' + e.__cause__
            _logger.log(e.level, message)
        if worker in self.recorders:
            self.health -= e.damage
            if self.health <= 0:
                self.dead = True

    def unexpected_damage(self, worker):
        _logger.expection('Unexpected exception occured when scraping cid %d', worker.cid)
        self.health -= _UNEXPECTED_DAMAGE
        if self.health <= 0:
            self.dead = True

    def is_dead(self):
        return self.dead

_MAX_HEALTH = 120
_REGEN_SPEED = 0.1
_UNEXPECTED_DAMAGE = 100