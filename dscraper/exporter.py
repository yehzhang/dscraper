import logging
import os
from xml.sax.saxutils import escape

from .utils import AutoConnector

_logger = logging.getLogger(__name__)


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
            Comment elements in XML are always omitted.
        """
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
        print('Comments from {}: \n'.format(cid))
        print(FileExporter.tostring(flow.get_document()))


class FileExporter(BaseExporter):
    """Save comments as XML files.

    :param str path: path to put files. Default as 'comments/' under the current
        working directory.
    :param bool merge: whether save all comments in one file along with history,
        or keep them separate as the original form. Notice: if choose not to
        merge files, the resulting files could take huge space because of
        duplication.
    """
    _OUT_DIR = 'comments'

    def __init__(self, path=None, merge=False, *, loop):
        super().__init__('Failed to save as files', loop=loop)
        if not path:
            path = self._OUT_DIR
        self._wd = self._home = path
        self._split = not merge

    async def dump(self, cid, flow, *, aid=None):
        # TODO if aid, dir: comments/av+aid/cid/*.xml
        self._cd()
        if not flow.has_history():
            _logger.debug('No history at cid %s', str(cid))
            latest = flow.get_latest()
        elif flow.can_split() and self._split:
            _logger.debug('History is splitted at cid %s', str(cid))
            self._cd(cid)
            for date, root in flow.get_histories():
                self._write(root, '{date},{cid}.xml'.format(cid=cid, date=date))
            latest = flow.get_latest()
        else:
            _logger.debug('History is merged at cid %s', str(cid))
            latest = flow.get_document()
        self._write(latest, '{cid}.xml'.format(cid=cid))

    async def _open_connection(self):
        pass

    def _cd(self, path=''):
        """Change working directory from home."""
        path = os.path.join(self._home, str(path))
        os.makedirs(path, exist_ok=True)
        self._wd = path

    # TODO performance if using run_in_executor?
    def _write(self, elements, filename):
        with open(os.path.join(self._wd, filename), 'w') as fout:
            fout.write(self.tostring(elements))

    @staticmethod
    def tostring(elements):
        lines = ['<?xml version="1.0" encoding="UTF-8"?>\n<i>']
        for elem in elements:
            text = escape(elem.text) if elem.text else ''
            if elem.tag == 'd':
                line = '\t<d p="{attrs}">{text}</d>'.format(attrs=elem.attrib['p'], text=text)
            else:
                line = '\t<{tag}>{text}</{tag}>'.format(tag=elem.tag, text=text)
            lines.append(line)
        lines.append('</i>')
        return '\n'.join(lines)


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

    async def dump(self, cid, flow, *, aid=None):
        pass

    async def _open_connection(self):
        pass

    async def disconnect(self):
        pass
