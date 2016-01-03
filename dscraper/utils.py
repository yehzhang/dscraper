import logging
def get_logger(name):
	logger = logging.getLogger(name)
	logger.addHandler(logging.NullHandler())
	return logger

def assert_false(predicate, logger, message, args):
	if not predicate:
		logger.debug(message, *args)
	return not predicate

from functools import update_wrapper
def decorator(d):
	def _d(f):
		return update_wrapper(d(f), f)
	return update_wrapper(_d, d)

