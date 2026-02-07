import sys
import os
import unittest
from bs4 import BeautifulSoup

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from transform_load import strip_html

class TestTransformLoad(unittest.TestCase):

    def test_strip_html_simple(self):
        html = "<p>Hello <b>World</b></p>"
        expected = "Hello World"
        self.assertEqual(strip_html(html), expected)

    def test_strip_html_none(self):
        self.assertIsNone(strip_html(None))

    def test_strip_html_empty(self):
        self.assertEqual(strip_html(""), "")

    def test_strip_html_no_tags(self):
        text = "Just text"
        self.assertEqual(strip_html(text), text)

    def test_strip_html_nested(self):
        html = "<div><p>Test</p><span>Span</span></div>"
        expected = "Test Span"
        self.assertEqual(strip_html(html), expected)

if __name__ == '__main__':
    unittest.main()
