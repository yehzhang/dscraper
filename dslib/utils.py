import logging

def get_logger(name):
	logger = logging.getLogger(__name__)
	logger.addHandler(logging.NullHandler())
	return logger


from functools import update_wrapper

def decorator(d):
	def _d(f):
		return update_wrapper(d(f), f)
	return update_wrapper(_d, d)

