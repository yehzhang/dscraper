"""
dscraper
~~~~~~~~
# TODO

:copyright: (c) 2016 by Simon Zhang.
:license: Apache 2.0, see LICENSE for more details.

"""

__title__ = 'dscraper'
__version__ = '0.2.0'
__author__ = 'Simon Zhang'
__license__ = 'Apache 2.0'
__copyright__ = 'Copyright 2016 Simon Zhang'

import sys
assert sys.version_info >= (3, 5)

from .exceptions import HostError, DecodeError, PageNotFound
from .scraper import Scraper
from .exporter import StdoutExporter, FileExporter, MysqlExporter, SqliteExporter

import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())
