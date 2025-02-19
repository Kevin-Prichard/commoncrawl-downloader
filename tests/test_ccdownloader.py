import unittest
from unittest.mock import Mock, patch, MagicMock
import json
from datetime import datetime
from collections import defaultdict
import gzip
import io

import ccdownloader
from ccdownloader import (
    CCIndexOfCrawls,
    CCIndexBuilder,
    CCPageLocator,
    CCPageDownloader,
    CCIndexStatusObserver,
    CCPageLocatorProgressObserver,
    CCPageLocatorPageObserver,
    PageLocatorProgress,
    PageLocatorObserver,
    UrlPattern,
    USER_AGENT
)

# Mock CDX_FLD_MAX for testing
ccdownloader.CDX_FLD_MAX = {
    'tld': 64,
    'domain': 128,
    'subdomain': 128,
    'path': 255,
    'headers': 511
}

class TestCCIndexOfCrawls(unittest.TestCase):
    @patch('ccdownloader.req_session')
    def test_run(self, mock_session):
        # Mock response data
        mock_crawls = [
            {"id": "CC-MAIN-2024-01", "name": "January 2024 Crawl"},
            {"id": "CC-MAIN-2024-02", "name": "February 2024 Crawl"}
        ]
        mock_response = Mock()
        mock_response.json.return_value = mock_crawls
        mock_session.get.return_value = mock_response

        # Create instance and run
        crawler = CCIndexOfCrawls(USER_AGENT)
        result = crawler.run()

        # Verify results
        self.assertEqual(len(result), 2)
        self.assertEqual(result["CC-MAIN-2024-01"]["name"], "January 2024 Crawl")
        self.assertEqual(result["CC-MAIN-2024-02"]["name"], "February 2024 Crawl")

        # Verify API call
        mock_session.get.assert_called_once_with(
            "https://index.commoncrawl.org/collinfo.json",
            headers={"user_agent": USER_AGENT}
        )

class MockCCIndexStatusObserver(CCIndexStatusObserver):
    def __init__(self):
        self.calls = []

    def __call__(self, crawl_label, status_msg, index_complete, indices_done, indices_total):
        self.calls.append({
            'crawl_label': crawl_label,
            'status_msg': status_msg,
            'index_complete': index_complete,
            'indices_done': indices_done,
            'indices_total': indices_total
        })

class TestCCIndexBuilder(unittest.TestCase):
    def setUp(self):
        self.observer = MockCCIndexStatusObserver()
        self.builder = CCIndexBuilder(
            label="CC-MAIN-2024-01",
            user_agent=USER_AGENT,
            observer=self.observer
        )

    @patch('ccdownloader.get_gz_resource')
    @patch('ccdownloader.Crawl')
    def test_get_cdx_urls(self, mock_crawl, mock_get_gz):
        # Mock gzipped content with valid CDX paths
        mock_get_gz.return_value = (
            "crawl-data/CC-MAIN-2024-01/segments/1234567890123.45/robotstxt/00000.gz\n"
            "crawl-data/CC-MAIN-2024-01/segments/1234567890123.45/wat/00000.gz\n"
            "crawl-data/CC-MAIN-2024-01/segments/1234567890123.45/wet/00000.gz\n"
            "crawl-data/CC-MAIN-2024-01/segments/1234567890123.45/cdx-00000.gz\n"
            "crawl-data/CC-MAIN-2024-01/segments/1234567890123.45/cdx-00001.gz\n"
            "crawl-data/CC-MAIN-2024-01/segments/1234567890123.45/cdx-00002.gz\n"
        )

        # Run test
        urls, count = self.builder._get_cdx_urls()

        # Verify results
        self.assertEqual(len(urls), 3)
        self.assertEqual(count, 3)
        self.assertTrue(all('cdx' in url for url in urls))

    @patch('ccdownloader.get_gz_resource')
    @patch('ccdownloader.Crawl')
    @patch('ccdownloader.CdxFirstUrl')
    def test_run(self, mock_cdx, mock_crawl, mock_get_gz):
        # Mock crawl
        mock_crawl.get.return_value = None
        mock_crawl.add.return_value = Mock(id=1)
        
        # Mock CDX operations
        mock_cdx.count.return_value = 0
        
        # Configure mock to return different values for different calls
        mock_get_gz.side_effect = [
            # First call - return CDX paths
            "crawl-data/CC-MAIN-2024-01/segments/1234567890123.45/cdx-00000.gz\n",
            # Second call - return CDX content that matches CDX_TO_URLPATTERN
            'com,example,www)/path 20240101123456 {"url":"http://www.example.com/path"}\n'
        ]

        # Mock getattr to handle dict access
        def mock_getattr(obj, name, default=None):
            return obj.get(name, default)
        
        # Patch getattr in ccdownloader module
        with patch('ccdownloader.getattr', mock_getattr):
            # Run test
            self.builder.run()

        # Verify observer calls
        self.assertTrue(any(call['status_msg'] == "Checking crawl index" 
                          for call in self.observer.calls))
        self.assertTrue(any(call['index_complete'] == True
                          for call in self.observer.calls))

class MockCCPageLocatorProgressObserver(CCPageLocatorProgressObserver):
    def __init__(self):
        self.calls = []

    def __call__(self, crawl_label, cdx_num, status_msg, index_complete, percent_done):
        self.calls.append({
            'crawl_label': crawl_label,
            'cdx_num': cdx_num,
            'status_msg': status_msg,
            'index_complete': index_complete,
            'percent_done': percent_done
        })

class MockCCPageLocatorPageObserver(CCPageLocatorPageObserver):
    def __init__(self):
        self.calls = []

    def __call__(self, page_info):
        self.calls.append(page_info)

class TestCCPageLocator(unittest.TestCase):
    @patch('ccdownloader.Crawl')
    def setUp(self, mock_crawl):
        self.progress_observer = MockCCPageLocatorProgressObserver()
        self.page_observer = MockCCPageLocatorPageObserver()
        self.url_patterns = [
            UrlPattern('com', 'example', None, None, None, None)
        ]
        
        # Mock Crawl.get to return a valid crawl
        mock_crawl.get.return_value = Mock(id=1)
        
        self.locator = CCPageLocator(
            crawl_label="CC-MAIN-2024-01",
            url_patterns=self.url_patterns,
            progress_observer=self.progress_observer,
            url_observer=self.page_observer
        )

    @patch('ccdownloader.Crawl')
    @patch('ccdownloader.CdxFirstUrl')
    def test_run(self, mock_cdx, mock_crawl):
        # Mock crawl
        mock_crawl.get.return_value = Mock(id=1)
        
        # Mock CDX operations
        mock_cdx.count.return_value = 0
        
        # Run test
        result = self.locator.run()

        # Verify progress observer calls
        self.assertTrue(any(call['status_msg'] == "Searching for CDXes"
                          for call in self.progress_observer.calls))
        self.assertTrue(any(call['index_complete'] == True
                          for call in self.progress_observer.calls))

    def test_url_patterns_to_regex(self):
        # Test regex pattern generation
        pattern = self.locator.url_patterns_to_regex()
        
        # Test matching - proper CDX line format matching CDX_TO_URLPATTERN
        test_line = 'com,example,www)/path 20240101123456 {"url": "http://www.example.com/path", "mime": "text/html", "status": "200", "length": "1000"}'
        self.assertTrue(pattern.match(test_line))
        
        # Test non-matching
        bad_line = 'org,badexample,www)/path 20240101123456 {"url": "http://www.badexample.org/path"}'
        self.assertFalse(pattern.match(bad_line))

class TestCCPageDownloader(unittest.TestCase):
    @patch('ccdownloader.Crawl')
    def setUp(self, mock_crawl):
        self.progress_observer = MockCCPageLocatorProgressObserver()
        self.page_observer = MockCCPageLocatorPageObserver()
        self.url_patterns = [
            UrlPattern('com', 'example', None, None, None, None)
        ]
        
        # Mock Crawl.get to return a valid crawl
        mock_crawl.get.return_value = Mock(id=1)
        
        self.locator = CCPageLocator(
            crawl_label="CC-MAIN-2024-01",
            url_patterns=self.url_patterns,
            progress_observer=self.progress_observer,
            url_observer=self.page_observer
        )
        self.downloader = CCPageDownloader(
            label="CC-MAIN-2024-01",
            page_locator=self.locator
        )

    @patch('ccdownloader.CCPageLocator.run')
    @patch('ccdownloader.CCPageLocator.filter_cdx_by_url')
    def test_run(self, mock_filter, mock_run):
        # Run test
        self.downloader.run()
        
        # Verify method calls
        mock_run.assert_called_once()
        mock_filter.assert_called_once()

class TestPageLocatorProgress(unittest.TestCase):
    def test_call(self):
        progress = PageLocatorProgress()
        
        # Test progress reporting
        progress("CC-MAIN-2024-01", 1, "Testing", False, 50.0)
        # No assertion needed as this just prints to console

class TestPageLocatorObserver(unittest.TestCase):
    @patch('dbschema.ccrawl.sa_session')
    def setUp(self, mock_session):
        self.mock_session = mock_session
        self.mock_sa = MagicMock()
        self.mock_session.return_value.__enter__.return_value = self.mock_sa
        self.observer = PageLocatorObserver("CC-MAIN-2024-01")

    @patch('ccdownloader.Crawl')
    @patch('ccdownloader.fetch_page_info')
    def test_call(self, mock_fetch, mock_crawl):
        # Mock database session
        self.mock_sa.execute.return_value = None
        self.mock_sa.commit.return_value = None
        
        # Mock Crawl.get to return a valid crawl for both get calls
        mock_crawl.get.return_value = Mock(id=1)
        
        # Mock fetch_page_info to return valid headers
        mock_fetch.return_value = {
            'url': 'https://data.commoncrawl.org/test.warc.gz',
            'headers': {
                'Content-Length': '1000'
            }
        }
        
        # Test page info handling with required fields
        page_info = {
            'filename': 'test.warc.gz',
            'url': 'http://example.com',
            'length': '1000',
            'mime': 'text/html',
            'status': '200'
        }
        
        # Add page_metadata field
        page_info['page_metadata'] = json.dumps(page_info)
        
        self.observer(page_info)
        
        # Verify counters
        self.assertEqual(self.observer.warc['test.warc.gz'], 1)
        self.assertEqual(self.observer.doms['example.com'], 1)

if __name__ == '__main__':
    unittest.main()
