__all__ = ()

import logging
import os

from .fetcher import CURRENT_URI, HISTORY_URI
from .utils import AutoConnector

_logger = logging.getLogger(__name__)

# File, Stream, MySQL, SQLite
# create dir, file

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
        self.connected = False

    async def _open_connection(self):
        self.connected = True

    async def disconnect(self):
        self.connected = False

    async def dump(self, cid, flow, *, aid=None):
        if not self.connected:
            return
        # TODO

class FileExporter(BaseExporter):
    """Save comments as XML files. The only exporter that supports splitting.
    """
    OUT_DIR = 'comments'
    # TODO maybe not necessary
    # :param bool merge: whether to save comments into files divided by dates
    #     as the website does, or to merge comments and save them as one file.

    def __init__(self, path=None, *, loop):
        super().__init__('Failed to save as files', loop=loop)
        if not path:
            path = self.OUT_DIR
        self._home = path
        self._sub_path = os.path.join(path, '{}')

    async def dump(self, cid, flow, *, aid=None):
        # TODO if aid, dir: comments/av+aid/cid/*.xml
        dirname = ''

        if flow.can_split():
            path = self._mkdir(cid)
            src = os.path.join(path, HISTORY_URI)
            for date, xml in flow.histories():
                xml.write(src.format(cid=cid, timestamp=date), "utf-8", True)

        root = flow.get_root()
        root.write(os.path.join(path, CURRENT_URI).format(cid=cid), "utf-8", True)

    async def _open_connection(self):
        self._mkdir('')

    async def disconnect(self):
        pass

    def _mkdir(self, dirname):
        path = os.path.join(self._home, str(dirname))
        os.mkdir(path, parents=True, exist_ok=True)
        return path

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