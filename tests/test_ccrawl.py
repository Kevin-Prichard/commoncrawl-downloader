from datetime import datetime

import unittest
from unittest.mock import Mock, patch, MagicMock
# import json

# from sqlalchemy import create_engine
# from sqlalchemy.orm import Session

# import pudb; pu.db

import common
common.make_engine = MagicMock()
common.sa_session = MagicMock()
common.engine = MagicMock()
from common import UrlPattern, sa_session

import sqlalchemy
sqlalchemy.create_engine = MagicMock()
sqlalchemy.make_session = MagicMock()

from sqlalchemy.engine import create
from sqlalchemy import orm
create.create_engine = MagicMock()
orm.Session = MagicMock()

# pu.db
# common.make_engine, common.sa_session, common.engine, orm.Session, create.create_engine, sqlalchemy.make_session, sqlalchemy.create_engine
from dbschema.ccrawl import (
    Base,
    Crawl,
    CdxFirstUrl,
    WebTextEmbeddings,
    KnownUrlPatterns,
    WarcResource,
    # sa_session
)


class TestCrawl(unittest.TestCase):
    # @patch('common.sa_session')
    def setUp(self):  #, mock_session):
        # common.sa_session = MagicMock()
        # self.mock_session = mock_session
        # self.mock_sa = MagicMock()
        # self.mock_session.return_value.__enter__.return_value = self.mock_sa
        
        self.test_label = "CC-MAIN-2024-01"
        self.test_url = "https://commoncrawl.org/crawl/CC-MAIN-2024-01"
        
    def test_get_by_label(self):
        # Mock query result
        mock_result = {
            'id': 1,
            'label': self.test_label,
            'url': self.test_url,
            'created': datetime.now()
        }
        # pu.db
        sa_session.return_value.__enter__.return_value.execute.return_value.mappings.return_value.first.return_value = mock_result
        
        # Test get by label
        crawl = Crawl.get(label=self.test_label)
        
        # Verify result
        self.assertIsNotNone(crawl)
        self.assertEqual(crawl.label, self.test_label)
        self.assertEqual(crawl.url, self.test_url)

    def test_get_by_id(self):
        # Mock query result
        mock_result = {
            'id': 1,
            'label': self.test_label,
            'url': self.test_url,
            'created': datetime.now()
        }
        sa_session.return_value.__enter__.return_value.execute.return_value.mappings.return_value.first.return_value = mock_result

        # Test get by id
        crawl = Crawl.get(crawl_id=1)

        # Verify result
        self.assertIsNotNone(crawl)
        self.assertEqual(crawl.label, self.test_label)

    def test_exists(self):
        # Mock get method
        with patch('dbschema.ccrawl.Crawl.get') as mock_get:
            mock_get.return_value = True
            
            # Test exists
            self.assertTrue(Crawl.exists(self.test_label))
            
            # Verify get was called
            mock_get.assert_called_once_with(label=self.test_label)
            
    def test_add(self):
        # Test add
        with patch('dbschema.ccrawl.Crawl.__table__.insert') as mock_insert:
            crawl = Crawl.add(self.test_label, self.test_url)

            mock_insert.assert_called_once()
            self.assertIsNotNone(crawl)
            sa_session.return_value.__enter__.return_value.query.return_value.filter_by.return_value.first.assert_called_once()
            sa_session.return_value.__enter__.return_value.execute.assert_called_once()
            sa_session.return_value.__enter__.return_value.commit.assert_called_once()


class TestCdxFirstUrl(unittest.TestCase):
    # @patch('common.sa_session')
    def setUp(self):  # , mock_session):
        self.mock_sa = sa_session.return_value.__enter__.return_value
        # self.mock_session = mock_session
        # self.mock_sa = MagicMock()
        # self.mock_session.return_value.__enter__.return_value = self.mock_sa
        
        self.test_crawl = Mock(id=1)
        self.test_cdx = CdxFirstUrl(
            crawl_id=1,
            cdx_num=1,
            tld='com',
            domain='example',
            subdomain='www',
            path='/',
            timestamp=datetime.now(),
            headers='{}'
        )
        
    def test_exists(self):
        # Mock query result
        # pu.db
        # sa_session.return_value.__enter__.return_value
        self.mock_sa.query.return_value.filter_by.return_value.first.return_value = self.test_cdx
        # self.mock_sa.query().filter_by().first.return_value = self.test_cdx
        
        # Test exists
        self.assertTrue(CdxFirstUrl.exists(self.test_crawl))
        
    def test_get(self):
        # Mock query result
        self.mock_sa.query.return_value.filter_by.return_value.all.return_value = [self.test_cdx]
        # self.mock_sa.query().filter_by().all.return_value = [self.test_cdx]
        
        # Test get
        result = CdxFirstUrl.get(self.test_crawl)
        
        # Verify result
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], self.test_cdx)
        
    def test_count(self):
        # Mock query count
        self.mock_sa.query.return_value.filter_by.return_value.count.return_value = 1
        # self.mock_sa.query().filter_by().count.return_value = 1
        
        # Test count
        count = CdxFirstUrl.count(self.test_crawl)
        
        # Verify result
        self.assertEqual(count, 1)
        
    def test_add_batch(self):
        # Test add_batch
        # sa = sa_session.return_value.__enter__.return_value
        self.mock_sa.query.return_value.filter_by.return_value.count.return_value = 1
        count = CdxFirstUrl.add_batch([self.test_cdx])
        self.assertEqual(count, 1)
        
        # Verify bulk_save_objects was called
        self.mock_sa.bulk_save_objects.assert_called_once_with([self.test_cdx])
        
        # Verify commit was called
        self.mock_sa.commit.assert_called_once()
        
    def test_find_domain_cdxes(self):
        # Mock execute results
        mock_result = Mock(cdx_num=1)
        """
        sa.execute(
                select(c.crawl, c.cdx_num, c.tld, c.domain)
                    .select_from(cls)
                    .where(and_(c.crawl == crawl,
                                c.tld < url.tld))
                    .order_by(c.tld.desc(), c.domain.desc())
                    .limit(1)
            ).first().cdx_num
        """
        self.mock_sa.execute.return_value.first.return_value = 1
        self.mock_sa.execute().mappings().all.return_value = [
            {
                'id': 1,
                'crawl_id': 1,
                'cdx_num': 1,
                'tld': 'com',
                'domain': 'example',
                'subdomain': 'www',
                'path': '/',
                'timestamp': datetime.now(),
                'headers': '{}',
                'created': datetime.now()
            }
        ]
        
        # Test find_domain_cdxes
        url_pattern = UrlPattern('com', 'example', None, None, None, None)
        results = CdxFirstUrl.find_domain_cdxes(1, url_pattern)
        
        # Verify results
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].domain, 'example')


class TestWebTextEmbeddings(unittest.TestCase):
    # @patch('dbschema.ccrawl.sa_session')
    def setUp(self):  # , mock_session):
        # self.mock_session = mock_session
        # self.mock_sa = MagicMock()
        # self.mock_session.return_value.__enter__.return_value = self.mock_sa
        
        self.test_embedding = WebTextEmbeddings(
            url='http://example.com',
            page_metadata='{"title": "Test"}',
            embedding=[0.1, 0.2, 0.3]
        )
        
    def test_exists(self):
        # Mock query result
        self.mock_sa.query().filter_by().first.return_value = self.test_embedding
        
        # Test exists
        self.assertTrue(self.test_embedding.exists())

class TestKnownUrlPatterns(unittest.TestCase):
    def setUp(self):
        self.test_pattern = KnownUrlPatterns(
            crawl_id=1,
            url='http://example.com',
            pattern='example.com/*',
            warc_count=0,
            warc_completed=False
        )
        
    def test_repr(self):
        # Test string representation
        repr_str = str(self.test_pattern)
        self.assertIn('example.com', repr_str)
        self.assertIn('example.com/*', repr_str)

class TestWarcResource(unittest.TestCase):
    @patch('dbschema.ccrawl.sa_session')
    def setUp(self, mock_session):
        self.mock_session = mock_session
        self.mock_sa = MagicMock()
        self.mock_session.return_value.__enter__.return_value = self.mock_sa
        
        self.test_crawl = Mock(id=1)
        self.test_page_url = 'http://example.com'
        self.test_warc_url = 'https://commoncrawl.org/warc/test.warc.gz'
        self.test_metadata = {'length': 1000}
        
    def test_add(self):
        # Test add
        WarcResource.add(
            self.test_crawl,
            self.test_page_url,
            self.test_warc_url,
            self.test_metadata,
            2000
        )
        
        # Verify execute was called with insert
        self.mock_sa.execute.assert_called_once()
        
        # Verify commit was called
        self.mock_sa.commit.assert_called_once()
        
    def test_exists(self):
        # Mock query result
        self.mock_sa.query().filter_by().first.return_value = True
        
        # Test exists
        self.assertTrue(WarcResource.exists(self.test_page_url))

if __name__ == '__main__':
    unittest.main()
