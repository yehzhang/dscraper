import unittest
import logging
import asyncio
import datetime
from pytz import timezone
import dscraper
from dscraper.utils import FrequencyController

logger = logging.getLogger(__name__)

from .utils import Test

EPS = 1e-6

class TestController(Test):

    INVERTAL = 0.2
    CONFIG_NONE = (0, 0, 0, EPS, None)
    CONFIG_ALL_DAY_NONE = (0, 0, 0, 0, None)
    CONFIG_INVALID = (0, INVERTAL, -0.1, 22, None)
    CONFIG_INVALID2 = (0, INVERTAL, 0, 24.1, None)

    def setUp(self):
        self.all_time = FrequencyController((0, self.INVERTAL, 0, 0, None))

    def wait_once(self, controller):
        return self.loop_until_complete(controller.wait())

    def test_wait(self):
        none_time = FrequencyController(self.CONFIG_NONE)
        all_none_time = FrequencyController(self.CONFIG_ALL_DAY_NONE)
        for cont in (self.all_time, none_time, all_none_time):
            self.assertFalse(self.wait_once(cont), 'First wait blocked')

        self.assertTrue(self.wait_once(self.all_time), 'False negative')
        self.assertFalse(self.wait_once(none_time), 'False positive')
        self.assertFalse(self.wait_once(all_none_time), 'False positive')

    def test_now_wait(self):
        now = datetime.datetime.now()
        start = end = now.hour + now.minute / 60 + now.second / 3600
        current = FrequencyController((0, self.INVERTAL, start - EPS, end + EPS, None))
        pos_offset = FrequencyController((0, self.INVERTAL, start - EPS, end - EPS, None))
        neg_offset = FrequencyController((0, self.INVERTAL, start + EPS, end + EPS, None))
        for cont in (current, pos_offset, neg_offset):
            self.assertFalse(self.wait_once(cont), 'First wait blocked')

        self.assertTrue(self.wait_once(current), 'False negative')
        self.assertFalse(self.wait_once(pos_offset), 'False positive')
        self.assertFalse(self.wait_once(neg_offset), 'False positive')

    def test_sequential(self):
        self.wait_once(self.all_time)

        self.all_time.release()
        self.assertCountEqual(self.gather(self.all_time.wait(), self.all_time.wait()), [
                              True, False], 'not released and acquired')

    def test_sequential(self):
        self.wait_once(self.all_time)

        self.assertTrue(self.wait_once(self.all_time), 'unblock before freed')
        self.all_time.free()
        self.assertFalse(self.wait_once(self.all_time), 'not freed')

    def test_invalid(self):
        def create_invalid(config):
            try:
                FrequencyController(config)
            except ValueError:
                pass
            else:
                self.fail('Incorrect value check')
        create_invalid(self.CONFIG_INVALID)
        create_invalid(self.CONFIG_INVALID2)
