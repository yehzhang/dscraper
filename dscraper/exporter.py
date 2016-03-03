__all__ = ()

import logging
import os
from xml.sax.saxutils import escape

from .utils import AutoConnector

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
    """Save comments as XML files.

    :param str path: path to put files. Default as 'comments/' under the current
        working directory.
    :param bool merge: whether save all comments in one file along with history,
        or keep them separate as the original form. Notice: if choose not to
        merge files, the resulting files could take huge space because of
        duplication.
    """
    _OUT_DIR = 'comments'
    _LAST_CMTS_FN = '{cid}.xml'
    _HIST_CMTS_FN = '{date},{cid}.xml'

    def __init__(self, path=None, merge=False, *, loop):
        super().__init__('Failed to save as files', loop=loop)
        if not path:
            path = self._OUT_DIR
        self._wd = self._home = path
        self._split = not merge

    async def dump(self, cid, flow, *, aid=None):
        # TODO if aid, dir: comments/av+aid/cid/*.xml
        # Has history? -> no dir, latest comments as one file
        # Otherwise, can split? -> dir, latest comments as one file, history comments as files
        # Otherwise -> no dir, all comments as one file
        # One file -> no dir, files -> dir
        self._cd('')
        latest_filename = self._LAST_CMTS_FN.format(cid=cid)
        if not flow.has_history():
            _logger.debug('No history cid %s', str(cid))
            self._write(flow.get_latest(), latest_filename)
        elif flow.can_split() and self._split:
            _logger.debug('Has splitter cid %s', str(cid))
            self._cd(cid)
            self._write(flow.get_latest(), latest_filename)
            for date, root in flow.get_histories():
                self._write(root, self._HIST_CMTS_FN.format(cid=cid, date=date))
        else:
            _logger.debug('Has history but no splitter, cid %s', str(cid))
            self._write(flow.get_document(), latest_filename)

    async def _open_connection(self):
        pass

    def _cd(self, path):
        """Change working directory from home."""
        path = os.path.join(self._home, str(path))
        os.makedirs(path, exist_ok=True)
        self._wd = path

    def _write(self, elements, filename):
        header_line = '\t<{tag}>{text}</{tag}>\n'
        cmt_line = '\t<d p="{attrs}">{text}</d>\n'

        with open(os.path.join(self._wd, filename), 'w') as fout:
            fout.write('<?xml version="1.0" encoding="UTF-8"?>\n<i>\n')
            for elem in elements:
                text = escape(elem.text) if elem.text else ''
                if elem.tag == 'd':
                    line = cmt_line.format(attrs=elem.attrib['p'], text=text)
                else:
                    line = header_line.format(tag=elem.tag, text=text)
                fout.write(line)
            fout.write('</i>')


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