__all__ = ('DscraperError', 'HostError', 'ConnectTimeout',
           'ResponseError', 'DataError', 'DecodeError', 'ParseError',
           'InvalidCid', 'MultipleErrors', 'NoResponseReadError')

from traceback import format_exception
from random import shuffle

_MAX_HEALTH = 120

class DscraperError(Exception):
    """A general error that Dscraper has nothing to do with it.
    Manual retrying is not recommended until the problem causing this error is
    solved, because Dscraper has retried automatically.
    """
    damage = 0 # too many exceptions triggered will kill the scraper

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

class ResponseError(HostError):
    """The response from the host was invalid."""

class NoResponseReadError(ResponseError):
    """There is no response from the host.
    Basically it is caused by opening a 404 page, or keeping the host waiting too long,
    so that the connection was closed. This exception occurs frequently."""
    damage = 10

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

class ParseError(DataError):
    """The string cannot be parsed as XML or JSON."""

class ContentError(DataError):
    """The recieved xml contains only a single element with 'error' as content."""
    damage = 0

class InvalidCid(DscraperError, ValueError, TypeError):
    """The worker was fed with an invalid cid to work with."""
    damage = 24

class MultipleErrors(DscraperError):
    """The container of multiple errors."""
    def __init__(self, errors):
        super().__init__()
        extract_info = lambda e: format_exception(e, e, None)[-1].rstrip('\n')
        message = '{} error(s) occured: \n{}'.format(len(errors),
                                                   ', \n'.join(map(extract_info, errors)))
        self.args = (message,)
        self.damage = max(errors, key=lambda e: e.damage)


class Life:

    def __init__(self):
        self.health = self._max_health = _MAX_HEALTH
        self.regen = _MAX_HEALTH / 10
        self.recorders = None

    def set_recorders(self, workers):
        num_workers = len(workers)
        shuffle(workers)
        num_recorders = min(num_workers, 3)
        self.recorders = frozenset(workers[:num_recorders])

        self.health = self._max_health = _MAX_HEALTH * recorders
        self.regen = _MAX_HEALTH / 10 * (recorders / num_workers)
        self.are_recorders_set = True

    def heal(self):
        self.health = min(self.health + self.regen, self._max_health)

    def damage(self, worker, e):
        if (self.recorders is None or work not in self.recorders) or e.damage <= 0:
            return
        self.health -= e.damage

    def is_dead(self):
        return self.health <= 0

