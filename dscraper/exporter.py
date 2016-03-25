import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from xml.sax.saxutils import escape

from .utils import AutoConnector

_logger = logging.getLogger(__name__)


class BaseExporter(AutoConnector):
    """Export an XML object to various destinations.

    :param string fail_result: what would happen if connection to the destination timed out
    """
    _CONNECT_TIMEOUT = 3.5

    def __init__(self, fail_result=None, *, loop):
        super().__init__(self._CONNECT_TIMEOUT, fail_result, loop=loop)

    async def dump(self, cid, flow, *, aid=None):
        """Export the data.

        :param int cid: chat ID, the identification number of the comments pool
            where the data came from

        note::
            Comment elements in XML are always omitted.
        """
        raise NotImplementedError


class StreamExporter(BaseExporter):
    """Write the output to a stream. The default stream is stdout."""

    def __init__(self, stream=None, end='\n', *, loop):
        super().__init__('Failed to write to the stream', loop=loop)
        self.stream = sys.stdout if stream is None else stream
        self.end = end

    async def _open_connection(self):
        pass

    async def disconnect(self):
        pass

    async def dump(self, cid, flow, *, aid=None):
        # TODO if aid, Comments from AID and CID
        self.write(self.stream, flow.get_document() if flow.has_history() else flow.get_latest())
        stream.write(self.end)

    @staticmethod
    def write(stream, elements):
        stream.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        stream.write('<i>\n')
        for elem in elements:
            text = escape(elem.text) if elem.text else ''
            stream.write('\t<d p="{attrs}">{text}</d>\n'.format(attrs=elem.attrib['p'], text=text)
                         if elem.tag == 'd' else
                         '\t<{tag}>{text}</{tag}>\n'.format(tag=elem.tag, text=text))
        stream.write('</i>')


class FileExporter(BaseExporter):
    """Save comments as XML files.

    :param str path: path to put files. Default as './comments'.
    :param bool merge: whether save all comments in one file along with history,
        or keep them separate as the original form. Notice: if choose not to
        merge files, the resulting files could take huge space because of
        duplication.
    """
    _OUT_DIR = 'comments'

    def __init__(self, path=None, merge=False, *, loop):
        super().__init__('Failed to save as files', loop=loop)
        if path is None:
            path = self._OUT_DIR
        self._home = os.path.abspath(path)
        self._split = not merge
        self._executor = ThreadPoolExecutor()

    async def dump(self, *args, **kwargs):
        await self.loop.run_in_executor(self._executor, self._dump, *args, **kwargs)

    def _dump(self, cid, flow, *, aid=None):
        # TODO if aid, dir: comments/av+aid/cid/*.xml
        wd = self._cd()
        if not flow.has_history():
            latest = flow.get_latest()
        elif flow.can_split() and self._split:
            wd = self._cd(cid)
            for date, root in flow.get_histories():
                self._write(root, wd, '{date},{cid}.xml'.format(cid=cid, date=date))
            latest = flow.get_latest()
        else:
            latest = flow.get_document()
        self._write(latest, wd, '{cid}.xml'.format(cid=cid))


    async def _open_connection(self):
        self._cd()

    async def disconnect(self):
        self._executor.shutdown()

    def _cd(self, path=''):
        """Change working directory from home."""
        return os.path.join(self._home, str(path))

    def _write(self, elements, wd, filename):
        os.makedirs(wd, exist_ok=True)
        with open(os.path.join(wd, filename), 'w') as fout:
            StreamExporter.write(fout, elements)


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
        raise NotImplementedError

    async def dump(self, cid, flow, *, aid=None):
        # TODO
        # d._user = int(user, 16)
        # d._is_tourist = (user[0] == 'D')
        pass

    async def _open_connection(self):
        pass

    async def disconnect(self):
        pass


class SqliteExporter(BaseExporter):

    def __init__(self, *, loop):
        super().__init__('Failed to insert into the database', loop=loop)
        raise NotImplementedError

    async def dump(self, cid, flow, *, aid=None):
        pass

    async def _open_connection(self):
        pass

    async def disconnect(self):
        pass


class MemoryExporter(BaseExporter):

    def __init__(self):
        raise NotImplementedError
