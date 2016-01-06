from random import shuffle

_MAX_HEALTH = 120

class DscraperError(Exception):
    damage = 0 # times workers can trigger exceptions before killing themselves

class ConnectionError(DscraperError, OSError):
    """An error occured while trying to communicate with the host for multiple times.

    Do not retry until the problem is fixed, which is probably that either
    your IP is blocked by the host, or your Internet connection is lost.
    """
    damage = 40

class ConnectTimeout(ConnectionError):
    """All attempts to connect to the host timed out."""

class ResponseError(ConnectionError):
    """The response from the host was invalid."""

class DataError(DscraperError, ValueError):
    """The data read from the response was invalid"""
    damage = 30

class DecodeError(DataError):
    """The byte array cannot be decoded."""
    # TODO what is its damage?

class ParseError(DataError):
    """The string cannot be parsed as XML or JSON."""

class InvalidCid(DscraperError, ValueError, TypeError):
    """The worker was fed with an invalid cid to work with."""
    damage = 24

class Life:

    def __init__(self):
        self.health = self._max_health = _MAX_HEALTH
        self.regen = _MAX_HEALTH / 10
        self.are_recorders_set = False

    def set_recorders(self, workers):
        num_workers = len(workers)
        recorders = min(num_workers, 3)
        for i, worker in enumerate(workers):
            worker._recorder = i < recorders
        shuffle(workers)

        self.health = self._max_health = _MAX_HEALTH * recorders
        self.regen = _MAX_HEALTH / 10 * (recorders / num_workers)
        self.are_recorders_set = True

    def heal(self):
        self.health = min(self.health + self.regen, self._max_health)

    def damage(self, worker, e):
        if (self.are_recorders_set and not work._recorder) or e.damage <= 0:
            return
        self.health -= e.damage

    def is_dead(self):
        return self.health <= 0