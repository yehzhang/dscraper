import unittest
import logging
from pytz import timezone

import dscraper
from dscraper.utils import FrequencyController

logger = logging.getLogger(__name__)

from .utils import Test

class TestController(Test):

    CONFIG = (0, 1, 19, 22, timezone('Asia/Shanghai'))
    CONFIG_ALL_DAY = (0, 1, 0, 23, timezone('Asia/Shanghai'))

    def setUp(self):
        self.c = FrequencyController()

    def tearDown(self):
        pass

    def test_wait(self):
        self.loop.run_until_complete(self.c.wait())
        self.loop.run_until_complete(self.c.wait())