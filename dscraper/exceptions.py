from random import shuffle

_MAX_HEALTH = 121
_HEALTH_REGEN = _MAX_HEALTH / 10

class DscraperError(Exception):
	pass

class ConnectionError(DscraperError, OSError):
	"""An error occured while trying to communicate with the host for multiple times.

	Do not retry until the problem is fixed, which is probably that either
	your IP is blocked by the host, or your Internet connection is lost.
	"""
	damage = 40

class ConnectTimeout(ConnectionError):
    """All attempts to connect to the host timed out."""

class NoResponseError(ConnectionError):
	"""No response was given by the host."""

class DataError(DscraperError, ValueError):
	"""The data read from the connection is invalid"""
	damage = 15

class DecodeError(DataError):
    """The byte array cannot be decoded."""
	# TODO what is its damage?

class ParseError(DataError):
    """The string cannot be parsed as XML or JSON."""

class InvalidCid(DscraperError, ValueError, TypeError):
	"""The worker was fed with an invalid cid to work with"""
	damage = 10


def _mark_recorders(workers):
	recorders = max(int(len(workers) / 3 + 0.5), 1)
	for i, worker in enumerate(workers):
	    worker.is_recorder = i < recorders
	shuffle(workers)

class _Health:

    def __init__(self, workers):
