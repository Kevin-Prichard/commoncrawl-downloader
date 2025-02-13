#!/usr/bin/env python3
import time
from abc import ABC, abstractmethod
from collections import defaultdict as dd
import gzip
import json
import os
import re
from datetime import datetime
import logging
from typing import Set, List, Union, Text, Generator, Mapping
from urllib.parse import urlsplit

import backoff
import dramatiq
from dramatiq import pipeline
from dramatiq.brokers.redis import RedisBroker
import httpx
from dramatiq.results import Results
from dramatiq.results.backends.redis import RedisBackend
from requests_cache import CachedSession

from common import (BACKOFF_EXCEPTIONS, BACKOFF_MAX_TIME, BACKOFF_MAX_TRIES)
from common import (on_backoff_rpt, on_success_rpt, on_giveup_rpt)
from common import (CC_TIMESTAMP_STRPTIME, CDX_RX,
                    CDX_TO_URLPATTERN, requests_session, UrlPattern)
from config import CC_DATA_HOSTNAME
from dbschema.ccrawl import (Crawl, CdxFirstUrl, WarcResource)
from gzip_partial import get_gz_resource
from simple_requests_cache import SimpleRequestsCache

# redis_broker = RedisBroker(host="localhost")
# result_backend = RedisBackend(host="localhost")
# redis_broker.add_middleware(Results(backend=result_backend))
# dramatiq.set_broker(redis_broker)

req_session = CachedSession('demo_cache')
logger = logging.getLogger(__name__)

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:100.0) "
              "Gecko/100.0 Firefox/100.0 (I'm the Browser You Deserve, "
              "Not the One You Want)")


def blank_none(val):
    return "" if val is None else val

URL_SCAN_BLOCKSIZE = 2**16
NEWLINE_BYTES = b"\n"
NEWLINE_STR = r"\n"
LINE_SPLITTER_BYTES = re.compile(b"(" + NEWLINE_BYTES + b")")
LINE_SPLITTER_STR = re.compile(r"(" + NEWLINE_STR + r")")

NEWLINE = NEWLINE_STR
LINE_SPLITTER = LINE_SPLITTER_STR

OBSERVER_FIELDS = ['tld', 'domain', 'subdomain', 'timestamp']


class CCIndexOfCrawls:
    def __init__(self, user_agent: str):
        self.user_agent = user_agent

    @backoff.on_exception(backoff.expo, BACKOFF_EXCEPTIONS,
                          max_time=BACKOFF_MAX_TIME,
                          max_tries=BACKOFF_MAX_TRIES,
                          on_success=on_success_rpt,
                          on_backoff=on_backoff_rpt,
                          on_giveup=on_giveup_rpt)
    def run(self):
        crawl_index = req_session.get(
            "https://index.commoncrawl.org/collinfo.json",
            headers={"user_agent": self.user_agent}
        ).json()
        return {crawl['id']: crawl for crawl in crawl_index}


class CCIndexStatusObserver(ABC):
    @abstractmethod
    def __call__(self,
                 crawl_label: str, status_msg: str, index_complete: bool,
                 indices_done: int, indices_total: int) -> None:
        """
        Whatever you do with this, do not block the main thread, or otherwise
        start a long-running synchronous process. This is for updating a
        progress bar, or logging, or something else that is quick and
        non-blocking.

        Args:
            crawl_label (str): CommonCrawl.org's label for the crawl.
            status_msg (str): The status message to be logged or displayed.
            index_complete (bool): Whether the indexing process is complete.
            indices_done (int): The number of indices processed so far.
            indices_total (int): The total number of indices to be processed.
        Returns:
            None
        """
        pass


class CCIndexBuilder:
    MAX_CDX_LENGTH_COMPRESSED = 2000
    MAX_CDX_LENGTH_UNCOMPRESSED = 2000
    def __init__(self,
                 label: str,
                 user_agent: str,
                 observer: CCIndexStatusObserver = None,
                 ):
        self.label = label
        self.user_agent = user_agent
        self.relevant_cdxes = None
        self.cdx_first_rows = None
        self._observer = observer
        self._start_timestamp = datetime.now()

    def run(self) -> None:
        crawl = Crawl.get(self.label)
        if not crawl:
            logger.info("Adding crawl %s", self.label)
            crawl = Crawl.add(self.label, self.label)
        self._send_obsv(
            "Checking crawl index",
            False, 0, 0)
        cdx_url_list, url_count = self._get_cdx_urls()
        if CdxFirstUrl.count(crawl) < url_count:
            self._send_obsv(
                "Fetching crawl indices",
                False, 0, url_count)
            self.cdx_first_rows, row_count = self._get_cdx_first_rows(
                crawl, cdx_url_list)
            self._send_obsv(
                "Saving crawl indices",
                False, row_count, url_count)
            if self.cdx_first_rows:
                self._save_cdx_first_rows()
            self._send_obsv(
                "Crawl indices received",
                True, row_count, url_count)

    def _get_cdx_urls(self) -> [List[str], int]:
        """
        Fetch the first row of each Common Crawl index file for label,
        and return as a list of the strings.  These will be alphanum sorted
        by (tld, domain, subdomain), and thus be used to binary search for a
        website within an entire CommonCrawl dataset.

        Args:
            label: Common Crawl index to fetch
        Returns:
            List[str]: The first row of each index file for the dataset
        """

        # Fetch the Common Crawl file of index file URL fragments
        cc_idx = get_gz_resource(
            f"https://{CC_DATA_HOSTNAME}/crawl-data/{self.label}"
            "/cc-index.paths.gz",
            user_agent=self.user_agent,
            requests_session=requests_session)

        # 1.1 Listify the URLs
        cdx_urls = list(filter(lambda url: CDX_RX.match(url),
                               cc_idx.strip().split('\n')))
        url_count = len(cdx_urls)
        logger.info("Found %d cdx files for %s",
                    len(cdx_urls), self.label)
        return cdx_urls, url_count

    def _get_cdx_first_rows(self,
                            crawl: Crawl,
                            cdx_urls: List[str]) -> [List[CdxFirstUrl], int]:
        # Fetch the first row of each index file, providing generous length
        # to ensure that the entire first row is included without error
        cdx_first_rows = []
        url_count = len(cdx_urls)

        for cdx_num, cdx_url_frag in enumerate(cdx_urls):
            if cdx_match := CDX_RX.match(cdx_url_frag):
                cdx_file_num = cdx_match.group(1)
            else:
                continue
            cdx_url = f"https://{CC_DATA_HOSTNAME}/{cdx_url_frag}"
            logger.debug("Fetching %s", cdx_url)
            self._send_obsv(
                "Fetching crawl index",
                False, cdx_num, url_count)
            cdx = get_gz_resource(
                cdx_url,
                user_agent=self.user_agent,
                requests_session=requests_session,
                length=self.MAX_CDX_LENGTH_COMPRESSED,
                max_uncompressed_length=self.MAX_CDX_LENGTH_UNCOMPRESSED,
            )
            cdx_lines = cdx.strip().split('\n')
            del cdx
            if not cdx_lines:
                logger.warning(f"No records found in {cdx_url}")
            rec = CDX_TO_URLPATTERN.match(cdx_lines[0])
            if not rec:
                pu.db
            rec = rec.groupdict()
            rec.update({'timestamp': datetime.strptime(rec['timestamp'],
                                                       CC_TIMESTAMP_STRPTIME),
                        'crawl_id': crawl.id,
                        'cdx_num': int(cdx_file_num),
                        'created': self._start_timestamp,
            })
            url_record = CdxFirstUrl(**rec)
            logger.debug("cdx first row %s from %s",
                         url_record, cdx_url_frag)
            cdx_first_rows.append(url_record)

        return cdx_first_rows, url_count

    def _save_cdx_first_rows(self):
        # if not Crawl.exists(self.label):
        added_count = CdxFirstUrl.add_batch(self.cdx_first_rows)
        logger.info("Added %d CdxFirstUrl records", added_count)

    def _send_obsv(self, status_msg: str, index_complete: bool,
                  indices_done: int, indices_total: int) -> None:
        if self._observer:
            self._observer(self.label, status_msg, index_complete,
                           indices_done, indices_total)

    """
    @staticmethod
    def pull_cdx_files(urls: Union[UrlPattern, List[UrlPattern]]) -> List[str]:
        found_cdxes = []
        urls = [urls] if isinstance(urls, UrlPattern) else urls
        for url in urls:
            found_cdxes.extend(CdxFirstUrl.find_domain_cdxes(url))
        cdx_urls = set(cdx.to_cdx_url() for cdx in found_cdxes)
        for cdx_url in cdx_urls:
            print(cdx_url)
        return found_cdxes
    """


class CCPageLocatorProgressObserver(ABC):
    @abstractmethod
    def __call__(self,
                 crawl_label: str,
                 cdx_num: int,
                 status_msg: str,
                 index_complete: bool,
                 percent_done: Union[float, None]) -> None:
        """
        Whatever you do with this, do not block the main thread, or otherwise
        start a long-running synchronous process. This is for updating a
        progress bar, or logging, or something else that is quick and
        non-blocking.

        Args:
            crawl_label (str): CommonCrawl.org's label for the crawl.
            status_msg (str): The status message to be logged or displayed.
            index_complete (bool): Whether the indexing process is complete.
            indices_done (int): The number of indices processed so far.
            indices_total (int): The total number of indices to be processed.

        Returns:
            None
        """
        pass


class CCPageLocatorPageObserver(ABC):
    @abstractmethod
    def __call__(self, page_info: Mapping) -> None:
        """
        Whatever you do with this, do not block the main thread, or otherwise
        start a long-running synchronous process. This is for a queue
        append or something else that is fast and non-blocking.

        Args:
            url (Mapping): The URL found in the CommonCrawl index.

        Returns:
            None
        """
        pass


class CCPageLocator:
    def __init__(self, crawl_label: str,
                 url_patterns: List[UrlPattern],
                 url_encoding: Text = None,
                 progress_observer: CCPageLocatorProgressObserver = None,
                 url_observer: CCPageLocatorPageObserver = None,
                 force_update: bool = False,
                 ):
        self.crawl_label = crawl_label
        self.crawl_id = Crawl.get(crawl_label).id
        self.url_patterns = url_patterns
        self.url_encoding = url_encoding
        self.progress_observer = progress_observer
        self.url_observer = url_observer
        self.force_update = force_update
        self.cdxes: Union[List[CdxFirstUrl], None] = None

    def run(self):
        self._send_progress(None, "Searching for CDXes",
                            False, None)
        cdx_count = 0
        existing_cdx_count = CdxFirstUrl.count(self.crawl_id)
        if self.force_update or existing_cdx_count == 0:
            self.cdxes = self._find_cdxes()
            cdx_count = len(self.cdxes)

        if cdx_count and not existing_cdx_count:
            msg = f"Found {cdx_count} new CDXes"
        elif existing_cdx_count:
            msg = f"Found {existing_cdx_count} existing CDXes"
        else:
            msg = "No CDXes found, new or existing"

        self._send_progress(None, msg, True, None)
        return cdx_count > 0

    def _find_cdxes(self) -> List[CdxFirstUrl]:
        """
        Find and return the `CdxFirstUrl` rows for cdx-NNNNN.gz files which
        likely contain the URL pattern(s) in self.url_patterns.

        Args: None

        Returns:
            List[CdxFirstUrl]: A list of `CdxFirstUrl` rows for
            cdx-NNNNN-gz files likely containing the given URL patterns.
        """
        cdxes = set()
        for url in self.url_patterns:
            cdxes.update(CdxFirstUrl.find_domain_cdxes(
                self.crawl_id,
                url
            ))
        return list(cdxes)

    def url_patterns_to_regex(self,
                              encoding: Text = None) -> re.Pattern:
        """
        Args:
            encoding: (str) The encoding to use for the regex pattern
        Returns:
            (re.Pattern): returns one, unified re.Pattern compiled from merger
                          of all the self.url_patterms (UrlPatterns)
        """
        eles = dict()
        for field in UrlPattern._fields:
            eles[field] = sorted(set(blank_none(getattr(url, field))
                                     for url in self.url_patterns))
        regex = (f"(?P<tld>{'|'.join(eles['tld'])})," +
                 f"(?P<domain>{'|'.join(eles['domain'])}),?" +
                 (f"(?P<subdomain>{'|'.join(eles['subdomain'])})\)" if eles[
                     'subdomain'] else "") +
                 (f"(?P<path>/?{'|'.join(eles['path'])}.*)" if eles[
                     'path'] else ".*") +
                 "\s+(?P<timestamp>\d+).*?" +
                 "\s+(?P<headers>\{.*\})$")
        if encoding:
            return re.compile(regex.encode(encoding), re.IGNORECASE)
        return re.compile(regex, re.IGNORECASE)

    def filter_cdx_by_url(self) -> Union[None, Generator[str, None, None]]:
        url_regex = self.url_patterns_to_regex(encoding=self.url_encoding)
        logger.warning("Searching CDXes for: %s", str(url_regex.pattern))
        reccount = 0
        bytecount = 0
        self._send_progress(None, "Begin scanning CDXes",
                            False, 0)
        for cdx in self.cdxes:
            stream_len = None
            stream = None
            if isinstance(cdx, Text):
                stream_len = os.stat(cdx).st_size
                stream = httpx.get(f"file:///home/kev/projs/mcp-loc/{cdx}",
                                   stream=True,
                                   headers={'accept-encoding': 'gzip'}
                                   )
            elif isinstance(cdx, CdxFirstUrl):
                # pu.db
                stream = SimpleRequestsCache(
                    "cache",
                    cdx.to_cdx_url(),
                    stream=True,
                    headers={'accept-encoding': 'gzip'})

                """
                response = requests.get(
                    cdx.to_cdx_url(), stream=True,
                    headers={'accept-encoding': 'gzip'})
                stream_len = int(response.headers.get('content-length'))
                stream = response.raw
                """

            if stream is None:
                raise ValueError(f"Invalid type {type(cdx)} for cdx")

            last_pct = 0.0
            with gzip.GzipFile(fileobj=stream, mode="rb") as gunzipped:
                partial = None
                skip = 0
                self._send_progress(None, "Starting CDX scan of %s" % cdx,
                                    False, 0.0)
                for block in iter(lambda: gunzipped.read(2 ** 22), b''):
                    if (new_pct := int(stream.tell() / stream.length() * 100)) > last_pct:
                        last_pct = new_pct
                        self._send_progress(cdx, "Scanning CDX",
                                            False, last_pct)
                    if not block:
                        print("empty block ", "*" * 80)
                    parts = LINE_SPLITTER.split(block.decode("utf-8"))
                    parts_end = len(parts) - 1
                    for ptr, part in enumerate(parts):
                        bytecount += len(part)
                        if ptr == 0 and partial:
                            part = partial + part
                            partial = None
                        if part != NEWLINE:
                            reccount += 1
                            if ptr == parts_end:
                                partial = part
                            elif rec_match := url_regex.match(part):
                                pieces = rec_match.groupdict()
                                page_info = json.loads(pieces['headers'])
                                page_info.update(dict(filter(
                                    lambda itm:itm[0] in OBSERVER_FIELDS,
                                    pieces.items()
                                )))
                                del pieces
                                if self.url_observer:
                                    self.url_observer(page_info)
                                # else:
                                #     yield page_headers
                            else:
                                skip += 1
                                if skip % 1000000 == 0:
                                    logger.info("skipped %d records", skip)
                self._send_progress(None, "Finished CDX scan of %s" % cdx,
                                    False, new_pct)

    def _send_progress(self, cdx: Union[CdxFirstUrl, None],
                       status_msg: str,
                       done: bool,
                       percent_done: Union[float, None]) -> None:
        if self.progress_observer:
            self.progress_observer(self.crawl_label,
                                   cdx.cdx_num if cdx else cdx,
                                   status_msg,
                                   done,
                                   percent_done)


class IndexStatus(CCIndexStatusObserver):
    def __call__(self,
                 crawl_label: str, status_msg: str,
                 index_complete: bool,
                 indices_done: int, indices_total: int) -> None:
        print(
            f"{crawl_label}: {status_msg} ({indices_done}/{indices_total})"
            f"{' DONE' if index_complete else ''}")


class CCPageDownloader:
    def __init__(self,
                 label: str,
                 page_locator: CCPageLocator,
                 # url_observer: CCPageLocatorPageObserver,
                 # progress_observer: CCPageLocatorProgressObserver,
                 ):
        self.crawl_label = label
        self.page_locator = page_locator
        # self.url_observer = url_observer
        # self.progress_observer = progress_observer

    def sum_downloads(self):
        for url_frag in self.page_locator.url_observer.warc:
            url = f"https://{CC_DATA_HOSTNAME}/{url_frag}"

    def run(self):
        self.page_locator.run()
        self.page_locator.filter_cdx_by_url()


class PageLocatorProgress(CCPageLocatorProgressObserver):
    def __call__(self,
                 crawl_label: str,
                 cdx_num: int,
                 status_msg: str,
                 index_complete: bool,
                 percent_done: Union[float, None]) -> None:
        print(f"{crawl_label}: {status_msg} ({cdx_num}) "
                f"{percent_done}%{' - DONE' if index_complete else ''}")


PIPELINE_SIZE = 48

class PageLocatorObserver(CCPageLocatorPageObserver):
    def __init__(self, crawl_label):
        self.crawl_label = crawl_label

        self.warc = dd(int)
        self.doms = dd(int)
        self.queue = []

    def __call__(self, page_headers: Mapping) -> None:
        self.warc[page_headers['filename']] += 1
        self.doms[urlsplit(page_headers['url']).netloc] += 1
        # print("Page Info: ", page_headers)
        # self.queue.append(push_page_info.message(self.crawl_label, page_headers))
        push_page_info(self.crawl_label, page_headers)
        # if len(self.queue) >= PIPELINE_SIZE:
        #     pipeline([self.queue.pop() for _ in range(PIPELINE_SIZE)]).run()
        #     print(f"Pipelined {PIPELINE_SIZE} messages")


# @dramatiq.actor
@backoff.on_exception(backoff.expo, BACKOFF_EXCEPTIONS,
                      max_time=BACKOFF_MAX_TIME,
                      max_tries=BACKOFF_MAX_TRIES,
                      on_success=on_success_rpt,
                      on_backoff=on_backoff_rpt,
                      on_giveup=on_giveup_rpt)
def fetch_page_info(url: str, *args, **kw) -> Mapping:
    print('fetch_page_info:', url, args, kw)
    response = req_session.head(url)
    if response.status_code == 200:
        return {
            'url': url,
            'headers': dict(response.headers),
        }
    else:
        logger.warning(f"Failed to fetch %s", url)


# @dramatiq.actor
def write_page_info(crawl_label: str,
                    page_headers: Mapping,
                    warc_url: str,
                    warc_info: Mapping,
                    ) -> None:
    crawl = Crawl.get(crawl_label)
    WarcResource.add(crawl=crawl,
                     page_url=page_headers['url'],
                     warc_url=warc_url,
                     metadata=page_headers,
                     length=int(warc_info['headers']['Content-Length']),
                     )


# @dramatiq.actor
def push_page_info(crawl_label: str, page_headers: Mapping) -> None:
    warc_url = f"https://{CC_DATA_HOSTNAME}/{page_headers['filename']}"
    if WarcResource.exists(warc_url):
        return

    warc_headers = fetch_page_info(warc_url)
    write_page_info(crawl_label,
                    page_headers=page_headers,
                    warc_url=warc_url,
                    warc_info=warc_headers
                    )

    # p = pipeline([
    #     fetch_page_info.message(warc_url),
    #     write_page_info.message(crawl_label, warc_url, page_headers),
    # ]).run()
    # while (result := p.get_result(block=True, timeout=60000)) and not result:
    #     time.sleep(1)


# @dramatiq.actor
def runner(crawl_label,
           url_patterns: Union[Set[UrlPattern], List[UrlPattern]] = None,):
    indexer = CCIndexBuilder(
        label=crawl_label,
        user_agent=USER_AGENT,
        observer=IndexStatus()
    )
    indexer.run()

    nuurls = set()
    for url in url_patterns:
        nuurls.add(UrlPattern(*url))

    page_locator = CCPageLocator(
         crawl_label,
         nuurls,
         progress_observer=PageLocatorProgress(),
         url_observer=PageLocatorObserver(crawl_label=crawl_label),
    )

    downloader = CCPageDownloader(
        crawl_label,
        page_locator=page_locator)
    downloader.run()
    # pu.db
    if page_locator.url_observer.queue:
        pipeline(page_locator.url_observer.queue).run()

    pl = page_locator
    logger.warning(f"%s: WARC urls: %d",
                   crawl_label, len(pl.url_observer.warc))
    logger.warning(f"%s: Target urls: %d",
                   crawl_label, len(pl.url_observer.doms))

    return page_locator


if __name__ == '__main__':
    # a = push_page_info(
    #     {"url":"https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-51/segments/1733066046748.1/warc/CC-MAIN-20241209152324-20241209182324-00392.warc.gz"}
    # )
    # print(a)
    # exit(0)
    # pu.db
    all_crawls = CCIndexOfCrawls(USER_AGENT).run()
    url_patterns = [
        # UrlPattern('com', 'rottentomatoes', None, None, None, None),
        # UrlPattern('com', 'alibaba', None, None, None, None),
        UrlPattern('com', 'gonze', None, None, None, None),
        # UrlPattern('com', 'gsmarena', None, None, None, None),
        # UrlPattern('com', 'nytimes', None, None, None, None),
    ]

    MAX_QUEUE = 24
    qqq = []
    # import pudb; pu.db
    for job_num, label in enumerate(all_crawls.keys()):
        if not (crawl := Crawl.exists(label)):
            crawl = Crawl.add(label, label)
        runner(crawl_label=label, url_patterns=url_patterns)
        # p = pipeline([runner.message(crawl_label=label,
        #                              url_patterns=url_patterns)])
        # p.run()
