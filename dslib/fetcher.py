import logging

logger = logging.getLogger(__name__)
# logging.addHandler(logging.NullHandler())


import requests

UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/600.7.12 (KHTML, like Gecko) Version/8.0.7 Safari/600.7.12'

MAX_REQUEST_TIMES = 5;
MAX_CIDS_SKIPPED = 10;
RELAX_DURATION = 1;


logger.warn('123')
print(__package__, __name__)
print(dir())