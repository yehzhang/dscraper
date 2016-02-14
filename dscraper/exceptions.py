# __all__ = ('DscraperError', 'HostError', 'ConnectTimeout',
#            'ResponseError', 'DataError', 'DecodeError', 'ParseError',
#            'InvalidCid', 'MultipleErrors', 'NoResponseReadError', 'PageNotFound')

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
    damage = 5

class MultipleErrors(DscraperError):
    """The container of multiple errors."""
    def __init__(self, errors):
        super().__init__('')
        errors = set((type(e), e.args[0], e.damage) for e in errors)
        self.types, messages, damages = zip(*errors)
        message = '{} distinct error(s) occured: \n{}'.format(len(errors), ';\n'.join(messages))
        self.args = (message,)
        self.damage = max(damages)


class Scavenger:
    """
    """
    _MAX_HEALTH = 120
    _REGEN_SPEED = 0.1
    _UNEXPECTED_DAMAGE = 100

    def __init__(self):
        self.dead = False
        self.health = self._max_health = self._MAX_HEALTH
        self.regen = self._max_health * self._REGEN_SPEED
        self.recorders = None

    def set_recorders_by(self, delta):
        if self.recorders is None:
            recorders = delta
            self.recorders = 1
        else:
            recorders = self.recorders + delta
        if recorders <= 0:
            raise ValueError('cannot set \'{}\' recorders'.format(recorders))

        self.health = self.health / self.recorders * recorders
        self._max_health = self._MAX_HEALTH * recorders
        self.regen = self._max_health * self._REGEN_SPEED * recorders
        self.recorders = recorders

    def success(self):
        self.health = min(self.health + self.regen, self._max_health)

    def failure(self, worker, e):
        # TODO log worker type
        if e is None:
            _logger.exception('Unexpected exception occured when scraping cid %d', worker.cid)
            self.health -= self._UNEXPECTED_DAMAGE
        else:
            message = '{} at cid {}'.format(self.capitalize(e.args[0]), worker.cid)
            if e.__cause__:
                message += ': ' + e.__cause__
            _logger.log(e.level, message)
            self.health -= e.damage
        if self.health <= 0:
            self.dead = True

    def is_dead(self):
        return self.dead

    @staticmethod
    def capitalize(s):
        return s[0].upper() + s[1:]

