__all__ = ()

import logging
import os

from .utils import AutoConnector, split_xml

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

    async def dump(self, *, cid=None, aid=None, xml=None, splitter=None):
        """Export the data.

        :param int cid: chat ID, the identification number of the comments pool
            where the data came from
        :param string text: the string to be dumped
        :param XML xml: the XML object to be dumped
        :param JSON splitter: the JSON object of Roll Date, which contains the information
            on how a large chunk of comment entries are divided into pieces

        note::
            At least one of the parameters text and xml must be supplied with data.
            If both parameters have data passed in, the exporter will choose which one to use,
            assuming that they are the same data in different format.
            If a splitter is provided, the data may be splitted into multiple parts
            on exporting. For example, FileExporter will save the comment entries as several
            files if a JSON object of Roll Date is provided. No splitter, no splitting.
        """
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

    async def dump(self, *, cid=None, aid=None, xml=None, splitter=None):
        if not self.connected:
            return
        # TODO

class FileExporter(BaseExporter):
    """Save comments as XML files. The only exporter that supports splitting.

    :param bool merge: whether to save comments into files divided by dates
        as the website does, or to merge comments and save them as one file.
    """
    _DIR_OUT = 'comments'

    def __init__(self, merge=False, path=None, *, loop):
        super().__init__('Failed to save as files', loop=loop)
        self.merge = merge
        if not path:
            path = self._DIR_OUT
        self.template = path + '/{}'

    async def dump(self, *, cid=None, aid=None, xml=None, splitter=None):
        if self.merge: # split
            splitter = None
        xmls = split_xml(xml, splitter)
        current_xml, history_xmls = xmls[0], xmls[1:]
        with open(self.template.format(cid + '.xml'), 'w') as fout:
            # fout.write(current_xml.)
            # TODO
            pass

    async def _open_connection(self):
        path = self.template.format('')
        if not os.path.exists(path):
            os.mkdir(path)

    async def disconnect(self):
        pass

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

    async def dump(self, *, cid=None, aid=None, xml=None, splitter=None):
        pass

    async def _open_connection(self):
        pass

    async def disconnect(self):
        pass

class SqliteExporter(BaseExporter):

    def __init__(self, *, loop):
        super().__init__('Failed to insert into the database', loop=loop)

    async def dump(self, *, cid=None, aid=None, xml=None, splitter=None):
        pass

    async def _open_connection(self):
        pass

    async def disconnect(self):
        pass