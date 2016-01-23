__all__ = ()

import logging

from .utils import AutoConnector

_logger = logging.getLogger(__name__)

# File, Stream, MySQL, SQLite
# create dir, file

# merge? autoflush/auto flush everytime?
class BaseExporter:

    def __init__(self, merge=False):
        self.buffer = []
        self.merge = merge

    async def open(self):
        raise NotImplementedError

    async def close(self):
        raise NotImplementedError

    async def add(self, xml):
        self.buffer.append(xml)
        if not self.merge:
            self.commit()

    async def commit(self):
        raise NotImplementedError

    def _merge(self):
        # TODO
        merged = None
        self.buffer.clear()
        return merged

class StdoutExporter(BaseExporter):

    def __init__(self, merge):
        super().__init__(merge)
        self.closed = False

    async def open(self):
        self.closed = False

    async def close(self):
        self.closed = True

    async def commit(self):
        print(self._merge())

class FileExporter(BaseExporter):

    def __init__(self):
        pass

class ConnectExporter(BaseExporter, AutoConnector):

    def __init__(self):
        pass

    async def open(self):
        self.connect()

class MysqlExporter(ConnectExporter):

    def __init__(self):
        pass

class SqliteExporter(ConnectExporter):

    def __init__(self):
        pass