__all__ = ()

import logging
import os

from .fetcher import CURRENT_COMMENTS_FILENAME, HISTORY_COMMENTS_FILENAME
from .utils import AutoConnector, serialize_comment_attributes

_logger = logging.getLogger(__name__)

# File, Stream, MySQL, SQLite

# merge? autoflush/auto flush everytime?
class BaseExporter(AutoConnector):
    """Export an XML object to various destinations.

    :param string fail_result: what would happen if connection to the destination timed out
    """
    def __init__(self, fail_result=None, *, loop):
        super().__init__(_CONNECT_TIMEOUT, fail_result, loop=loop)

    async def dump(self, cid, flow, *, aid=None):
        """Export the data.

        :param int cid: chat ID, the identification number of the comments pool
            where the data came from

        note::
            If a splitter is provided, the data may be splitted into multiple parts
            on exporting. For example, FileExporter will save the comment entries as several
            files if a JSON object of Roll Date is provided. No splitter, no splitting.
        """
        # :param XML header: elements attached at the top of each file,
        #     usually metadata of CID
        # :param XML body: joined elements at the center of all files,
        #     usually unique, sorted, normal comments
        # :param XML footer: elements attached at the bottom of each file,
        #     usually protected comments
        # :param JSON splitter: how body should be splitted, usually Roll Date, which
        #     contains the information on how a large chunk of comment entries
        #     are divided into pieces.
        raise NotImplementedError

_CONNECT_TIMEOUT = 3.5

class StdoutExporter(BaseExporter):
    """Prints human-readable comment entries to stdout."""

    def __init__(self, *, loop):
        super().__init__('Failed to print to the console', loop=loop)

    async def _open_connection(self):
        pass

    async def disconnect(self):
        pass

    async def dump(self, cid, flow, *, aid=None):
        print('All comments from {} are the following: '.format(cid))
        # TODO
        print()

class FileExporter(BaseExporter):
    """Save comments as XML files. The only exporter that supports splitting.
    """
    _OUT_DIR = 'comments'
    # TODO maybe not necessary
    # :param bool merge: whether to save comments into files divided by dates
    #     as the website does, or to merge comments and save them as one file.

    def __init__(self, path=None, *, loop):
        super().__init__('Failed to save as files', loop=loop)
        if not path:
            path = self._OUT_DIR
        self._wd = self._home = path

    async def dump(self, cid, flow, *, aid=None):
        # TODO if aid, dir: comments/av+aid/cid/*.xml
        if flow.can_split():
            # Normal mode, export all comments in a file structure similar to the original one
            _logger.debug('Normal file export cid %s', str(cid))
            self._cd(cid)
            xml.write(flow.get_latest(), CURRENT_COMMENTS_FILENAME.format(cid=cid))
            for date, xml in flow.histories():
                self._write(xml, HISTORY_COMMENTS_FILENAME.format(cid=cid, timestamp=date))
        else:
            # Deviated mode. Either there is no history, or the time range is set.
            # When I say there is no history, I mean the comments are too short to have
            # history. Although there may be Roll Dates, dscraper does not request it
            # for speed.
            # No folder is created for each file
            _logger.debug('Deviated file export cid %s', str(cid))
            self._write(flow.all(), CURRENT_COMMENTS_FILENAME.format(cid=cid))

    async def _open_connection(self):
        self._cd('')

    def _cd(self, dirname):
        path = os.path.join(self._home, str(dirname))
        os.makedirs(path, exist_ok=True)
        self._wd = path

    def _write(self, xml, filename):
        serialize_comment_attributes(xml)
        xml.write(os.path.join(self._wd, filename), "utf-8", True)

class MysqlExporter(BaseExporter):
    """Intended features:
        auto-reconnect on connection lost,
        switch to a new table the current one contains to many rows
    """

    def __init__(self, *, loop):
        super().__init__('Failed to insert into the database', loop=loop)
        # TODO wait until connect
        # if cmtdb is not created, create and set encoding
        # SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

    async def dump(self, cid, flow, *, aid=None):
        pass

    async def _open_connection(self):
        pass

    async def disconnect(self):
        pass

class SqliteExporter(BaseExporter):

    def __init__(self, *, loop):
        super().__init__('Failed to insert into the database', loop=loop)

    async def dump(self, cid, flow, *, aid=None):
        pass

    async def _open_connection(self):
        pass

    async def disconnect(self):
        pass