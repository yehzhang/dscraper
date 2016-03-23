import logging
import asyncio
import xml.etree.ElementTree as et
from dscraper.company import CommentWorker
from dscraper.utils import parse_comments_xml, parse_rolldate_json, CommentFlow

logger = logging.getLogger(__name__)

from .utils import Test


class TestCommentFlow(Test):

    def setUp(self):
        # def __init__(self, latest, histories, flows, roll_dates, limit):
        pass

    def test_get_all_comments(self):
        pass

    def test_get_histories(self):
        pass

    def test_get_document(self):
        pass
