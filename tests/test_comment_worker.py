import logging
from itertools import chain
from xml.etree.ElementTree import Element
from dscraper.company import CommentWorker
from dscraper.scraper import BlockingDistributor
from dscraper.utils import parse_comments_xml

logger = logging.getLogger(__name__)

from .utils import Test

CMTS = 'comments'
RD = 'roll_date'
DUMP = 'dump'
MAX_LIMIT = 'maxlimit'
DS = 'start_date'
DE = 'end_date'
TARG = 'target_histories'
HIST = 'history'
LAST = 'latest'
SEGM = 'segments'

XML_TEMPLATE = '<i><maxlimit>{}</maxlimit><ds>{}</ds>{}</i>'
CMT_TEMPLATE = '<d p="{}"></d>'


def make_xml(cmts, maxlimit=0, ds=0):
    str_cmts = ''.join(CMT_TEMPLATE.format(cmt_tostring(cmt)) for cmt in cmts)
    return parse_comments_xml(XML_TEMPLATE.format(maxlimit, ds, str_cmts))


def cmt_tostring(cmt):
    def _tostring(id, *, date=None, offset=0, mode=0, font_size=0, color=0, pool=0, user=0):
        if date is None:
            date = id
        return ','.join(str(attr) for attr in (offset, mode, font_size, color, date, pool, user, id))
    return _tostring(cmt) if isinstance(cmt, int) else _tostring(**cmt)


class ActionRecorder:

    def __init__(self):
        self.actions = []

    def record(self, name, action):
        self.actions.append((name, action))

    def record_cmts(self, cid, date):
        self.actions.append(self.make_cmts_action(cid, date))

    def record_rd(self, cid):
        self.actions.append(self.make_rd_action(cid))

    def get_actions(self):
        return self.actions

    @staticmethod
    def make_cmts_action(cid, date):
        return (CMTS, (cid, date))

    @staticmethod
    def make_rd_action(cid):
        return (RD, (cid,))


class CidInfo:

    def __init__(self, cid, data):
        latest_date = data[LAST]
        self.maxlimit = data.get(MAX_LIMIT, len(latest_date))
        self.ds = data.get(DS, 0)
        rd = self.roll_date = []
        dates = self.dates = {0: latest_date}
        for cmts in data[HIST]:
            date = cmts[-1]
            rd.append(date)
            dates[date] = cmts
        AR = ActionRecorder
        self.actions = [AR.make_cmts_action(cid, 0)]
        if latest_date[0] >= self.ds and len(latest_date) >= self.maxlimit:
            # has history
            self.actions.extend([AR.make_rd_action(cid)])
            self.actions.extend([AR.make_cmts_action(cid, rd[ordinal - 1])
                                 for ordinal in reversed(data[TARG])])

STUB_DATA_GENERAL = {k: CidInfo(k, v) for k, v in {
    1: {
        TARG: [1, 2, 3, 4, 5],  # ordinal of history
        HIST: [[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]],
        LAST: [11, 12]
    },
    2: {
        TARG: [1, 2, 3, 4, 5],
        HIST: [[1, 2], [2, 5], [5, 7], [7, 9], [7, 10]],
        LAST: [10, 12]
    },
    2.1: {
        TARG: [1, 2, 3, 5],
        HIST: [[1, 4], [4, 5], [5, 7], [7, 9], [6, 10]],
        LAST: [10, 12]
    },
    2.2: {
        TARG: [1, 3, 5],
        HIST: [[1, 3], [3, 5], [2, 7], [7, 9], [6, 10]],
        LAST: [10, 12]
    },
    3: {
        TARG: [1],
        HIST: [[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]],
        LAST: [1, 12]
    },
    3.1: {
        TARG: [1, 2],
        HIST: [[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]],
        LAST: [2, 12]
    },
    4: {
        TARG: [1, 5],
        DS: 1,
        HIST: [[1, 3], [3, 4], [5, 6], [7, 8], [1, 10]],
        LAST: [11, 12]
    },
    4.1: {
        TARG: [5],
        DS: 2,
        HIST: [[1, 3], [3, 4], [5, 6], [7, 8], [1, 10]],
        LAST: [11, 12]
    },
    5: {
        TARG: [1],
        DS: 1,
        HIST: [[1, 3], [3, 4]],
        LAST: [1, 12]
    },
    5.1: {
        TARG: [],
        DS: 2,
        HIST: [[1, 3], [3, 4]],
        LAST: [1, 12]
    },
    5.5: {
        TARG: [1, 2],
        DS: 3,
        HIST: [[1, 3], [3, 4]],
        LAST: [3, 12]
    },
    6: {
        TARG: [],
        MAX_LIMIT: 2,
        HIST: [[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]],
        LAST: [12]
    },
    6.1: {
        TARG: [1, 2, 3, 4, 5],
        MAX_LIMIT: 1,
        HIST: [[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]],
        LAST: [10, 12]
    }
}.items()}


STUB_DATA_DIGEST = [
    {
        TARG: ([3, 6, 9], [2, 5, 8], [1], [4, 7]),
        CMTS: [
            3, 6, 9, 2, 5, 8,
            {'id': 1, 'pool': 1},
            {'id': 4, 'pool': 2},
            {'id': 7, 'pool': 2},
        ]
    }, {
        TARG: ([3, 6, 9, 9], [], [], [7]),
        CMTS: [
            3, 6, 9, 9,
            {'id': 7, 'pool': 2},
        ]
    }, {
        TARG: ([2], [1], [], []),
        CMTS: [
            2, 1
        ]
    }, {
        TARG: ([1], [], [7], []),
        CMTS: [
            1,
            {'id': 7, 'pool': 1}
        ]
    }
]

STUB_DATA_JOIN = [
    {
        TARG: list(range(1, 9)),
        SEGM: [
            [1, 2, 3], [2, 3, 4], [5, 6, 7], [], [8]
        ]
    }, {
        TARG: [1, 3, 4, 5, 7],
        SEGM: [
            [1, 3], [2, 3, 4], [5, 7]
        ]
    }, {
        TARG: list(range(1, 8)),
        SEGM: [
            [1, 2, 3, 4, 5], [3, 4, 5, 6], [4, 5, 6, 7]
        ]
    }
]

STUB_DATA_TRIM = [
    {
        TARG: list(range(5, 11)),
        DS: 5,
        DE: 10,
        CMTS: list(range(100))
    }, {
        TARG: list(range(3, 10)),
        DS: 3,
        DE: 7,
        CMTS: [
            1, 2, 3,
            {'id': 4, 'date': 3},
            {'id': 5, 'date': 3},
            6, 7,
            {'id': 8, 'date': 7},
            {'id': 9, 'date': 7},
            10
        ]
    }
]

class TestCommentWorker(Test):

    def setUp(self):
        self.maxDiff = None
        self.dtor = BlockingDistributor(loop=self.loop)
        self.etor = DummyExporter()
        self.sger = DummyScavenger()
        self.worker = CommentWorker(distributor=self.dtor, scavenger=self.sger, exporter=self.etor,
                                    history=True, loop=self.loop, time_range=(None, None))
        self.worker.fetcher = self.fcer = DummyFetcher()

    def test_general(self):
        """Test general logic from stub data."""
        self.dtor.post(STUB_DATA_GENERAL.keys())
        self.dtor.set()
        self.loop.run_until_complete(self.worker.run())

        self.assertFalse(self.sger.get_actions(), 'exception caught by worker during the test')
        self.assertEqual(self.fcer.get_actions(), list(
            chain(*(i.actions for i in STUB_DATA_GENERAL.values()))), 'incorrect actions')
        actions = self.etor.get_actions()
        self.assertEqual(len(actions), len(STUB_DATA_GENERAL), 'excessive dump')
        for (_, (dumped_cid, _, _)), cid in zip(actions, STUB_DATA_GENERAL.keys()):
            self.assertEqual(dumped_cid, cid, 'incorrect target cid')

    def test_digest(self):
        for data in STUB_DATA_DIGEST:
            segments = self.worker._digest(make_xml(data[CMTS]))
            for segment in segments:
                segment[:] = (elem.attrib['id'] for elem in segment)
            self.assertEqual(segments, data[TARG], 'incorrect digestion')

    def test_join(self):
        for data in STUB_DATA_JOIN:
            segments = data[SEGM]
            for segment in segments:
                for i, elem in enumerate(segment):
                    new_elem = Element('')
                    new_elem.attrib['id'] = elem
                    segment[i] = new_elem
            self.assertEqual(self.get_id_list(self.worker._join(segments)), data[TARG], 'incorrect joining')

    def test_trim(self):
        for data in STUB_DATA_TRIM:
            flow = make_xml(data[CMTS]).findall('d')
            self.worker._trim(flow, data[DS], data[DE])
            self.assertEqual(self.get_id_list(flow), data[TARG])

    @staticmethod
    def get_id_list(elems):
        return [elem.attrib['id'] for elem in elems]

class DummyFetcher(ActionRecorder):

    async def get_comments_root(self, cid, date=0):
        self.record_cmts(cid, date)
        data = STUB_DATA_GENERAL[cid]
        return make_xml(data.dates[date], data.maxlimit, data.ds)

    async def get_rolldate_json(self, cid):
        self.record_rd(cid)
        return STUB_DATA_GENERAL[cid].roll_date

    async def __aenter__(self):
        pass

    async def __aexit__(self, exc_type, exc, tb):
        pass


class DummyExporter(ActionRecorder):

    async def dump(self, cid, flow, *, aid=None):
        self.record(DUMP, (cid, flow, aid))


class DummyScavenger(ActionRecorder):

    def is_dead(self):
        return False

    def success(self):
        pass

    def failure(self, worker, e):
        logger.exception('dummy scavenger caught an exception')
        self.record('Exception', (worker, e))
