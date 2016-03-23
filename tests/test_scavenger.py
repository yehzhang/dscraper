import logging
import asyncio
import dscraper
from dscraper.exceptions import Scavenger

from .utils import Test

logger = logging.getLogger(__name__)


class TestScavenger(Test):

    def setUp(self):
        self.s = Scavenger()

    def test_failures(self):
        raise NotImplementedError
