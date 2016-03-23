import unittest
import logging
import xml.etree.ElementTree as et

import dscraper.utils as utils

logger = logging.getLogger(__name__)

class TestUtils(unittest.TestCase):

    XML_FILES = (
        'tests/resources/1.xml',
    )

    def setUp(self):
        raise NotImplementedError

