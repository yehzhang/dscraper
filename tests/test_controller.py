import unittest
import logging
import datetime
from pytz import timezone
import dscraper
from dscraper.utils import FrequencyController

logger = logging.getLogger(__name__)

from .utils import Test

class TestController(Test):

    CONFIG_NONE = (0, 0, 0, 1e-9, None)
    CONFIG_ALL_DAY = (0, 1, 0, 0, None)
    CONFIG_ALL_DAY_NONE = (0, 0, 0, 0, None)
    CONFIG_INVALID = (0, 1, -0.1, 22, None)
    CONFIG_INVALID2 = (0, 1, 0, 24.1, None)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_wait(self):
        all_time = FrequencyController(self.CONFIG_ALL_DAY)
        self.assertTrue(self.loop.run_until_complete(all_time.wait()), 'False negative')
        none_time = FrequencyController(self.CONFIG_NONE)
        self.assertFalse(self.loop.run_until_complete(none_time.wait()), 'False positive')
        all_none_time = FrequencyController(self.CONFIG_ALL_DAY_NONE)
        self.assertFalse(self.loop.run_until_complete(all_none_time.wait()), 'False positive')

    def test_now_wait(self):
        now = datetime.datetime.now()
        start = end = now.hour + now.minute / 60 + now.second / 3600
        current = FrequencyController((0, 1, start - 0.1, end + 0.1, None))
        pos_offset = FrequencyController((0, 1, start - 0.1, end - 0.01, None))
        neg_offset = FrequencyController((0, 1, start + 0.01, end + 0.1, None))
        self.assertTrue(self.loop.run_until_complete(current.wait()), 'False negative')
        self.assertFalse(self.loop.run_until_complete(pos_offset.wait()), 'False positive')
        self.assertFalse(self.loop.run_until_complete(neg_offset.wait()), 'False positive')


    def test_invalid(self):
        try:
            FrequencyController(self.CONFIG_INVALID)
        except ValueError:
            pass
        else:
            self.fail('Incorrect value check')
        try:
            FrequencyController(self.CONFIG_INVALID2)
        except ValueError:
            pass
        else:
            self.fail('Incorrect value check')
