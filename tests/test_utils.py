import unittest
import logging
import xml.etree.ElementTree as et

import dscraper.utils as utils

logger = logging.getLogger(__name__)

class Test_Utils(unittest.TestCase):

    XML_FILES = (
        'tests/resources/1.xml',
    )

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_serializer(self):
        filename = self.XML_FILES[0]
        xml = et.parse(filename)
        utils.deserialize_comment_attributes(xml)
        utils.serialize_comment_attributes(xml)
        ref_xml = et.parse(filename)
        self.assertTrue(all(d1.attrib['p'] == d2.attrib['p'] for d1, d2 in
                        zip(xml.iterfind('d'), ref_xml.iterfind('d'))))
