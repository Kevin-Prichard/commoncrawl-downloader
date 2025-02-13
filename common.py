from collections import namedtuple
import logging
from functools import partial

from pytz import timezone
import re
from typing import Mapping, List, Union, Iterable

import backoff
from clickhouse_sqlalchemy import make_session, get_declarative_base
from requests import RequestException, HTTPError, Timeout, ConnectionError
from requests.sessions import Session
from sqlalchemy import create_engine

from config import CACHE_REQUESTS, CH_CONNECT_URI


logger = logging.getLogger(__name__)


if CACHE_REQUESTS:
    from requests_cache import CachedSession
    requests_session: Session = CachedSession('demo_cache')
else:
    import requests
    requests_session: Session = requests.Session()

CDX_RX = re.compile(r'.*/cdx-(\d{5}).gz$')
CDX_URL_TEMPLATE = ("https://{hostname}/cc-index/collections/"
                    "{label}/indexes/cdx-{cdx_num:05d}.gz")
UrlPattern = namedtuple(
    'Url',
    ['tld', 'domain', 'subdomain', 'path', 'timestamp', 'headers'])

# Example:
# com,greencarcongress)/2024/06/20240620-abcdef.html 20241209101212
# {"url": "https://www.example.com/2024/06/20240620-lhyfe.html",
# "mime": "text/html", "mime-detected": "text/html", "status": "200",
# "digest": "ZT5X2CTZ2PGASDFKLJGH3BCBXZLBHGOD", "length": "14883",
# "offset": "707350000",
# "filename": "crawl-data/CC-MAIN-2024-51/segments/1733063453214.97/warc/CC-MAIN-20241209085821-20241209115821-00640.warc.gz",
# "charset": "UTF-8", "languages": "eng"}
CDX_TO_URLPATTERN = re.compile(
    r"^(?P<tld>[^,]+?),(?P<domain>[^,]+?),?(?P<subdomain>[^)]*)\)"
    r"(?P<path>.*?)\s+(?P<timestamp>\d+)\s+(?P<headers>{.+}?)$")  # <== Note*

""" Note*: the option '?' after the last JSON '}' on CDX_TO_URLPATTERN is 
    there bc we really only want tld, domain, subdomain and path for CDX first 
    rows. The headers are recorded in CdxFirstUrl table but not used anywhere.
"""


"""
re.compile(r"^(?P<tld>[^,]+?),(?P<domain>[^,]+?),?(?P<subdomain>[^)]*)\)(?P<path>\S*)\s+(?P<timestamp>\d+)\s+(?P<headers>{.+})$")
)
a='org,bvsalud)/en/multimedia?filter=descriptor:"coronavirus%20infections"%20and%20descriptor:"coronavirus"%20and%20media_type_filter:"pt-br^imagem%20fixa|es^imagen%20fija|en^still%20image|fr^image%20fixe"%20and%20descriptor:"mental%20health"%20and%20descriptor:"coronavirus"%20and%20descriptor:"coronavirus%20infections"%20and%20descriptor:"mental%20health"%20and%20descriptor:"coronavirus"%20and%20descriptor:"mindfulness"%20and%20descriptor:"mindfulness"%20and%20descriptor:"mental%20health"%20and%20media_collection_filter:"colnal"%20and%20descriptor:"mental%20health"%20and%20media_collection_filter:"colnal"%20and%20descriptor:"coronavirus%20infections"%20and%20descriptor:"coronavirus"%20and%20media_collection_filter:"colnal"%20and%20descriptor:"coronavirus"%20and%20descriptor:"coronavirus"%20and%20media_type_filter:"pt-br^imagem%20fixa|es^imagen%20fija|en^still%20image|fr^image%20fixe" 20241114042049 {"url": "https://bvsalud.org/en/multimedia/?filter=descriptor:%22Coronavirus%20Infections%22%20AND%20descriptor:%22Coronavirus%22%20AND%20media_type_filter:%22pt-br%5EImagem%20fixa%7Ces%5EImagen%20fija%7Cen%5EStill%20image%7Cfr%5EImage%20fixe%22%20AND%20descriptor:%22Mental%20Health%22%20AND%20descriptor:%22Coronavirus%22%20AND%20descriptor:%22Coronavirus%20Infections%22%20AND%20descriptor:%22Mental%20Health%22%20AND%20descriptor:%22Coronavirus%22%20AND%20descriptor:%22Mindfulness%22%20AND%20descriptor:%22Mindfulness%22%20AND%20descriptor:%22Mental%20Health%22%20AND%20media_collection_filter:%22COLNAL%22%20AND%20descriptor:%22Mental%20Health%22%20AND%20media_collection_filter:%22COLNAL%22%20AND%20descriptor:%22Coronavirus%20Infections%22%20AND%20descriptor:%22Coronavirus%22%20AND%20media_collection_filter:%22COLNAL%22%20AND%20descriptor:%22Coronavirus%22%20AND%20descriptor:%22Coronavirus%22%20AND%20media_type_filter:%22pt-br%5EImagem%20fixa%7Ces%5EImagen%20fija%7Cen%5EStill%20image%7Cfr%5EImage%20fixe%22", "mime": "text/html", "mime-detected": "text/html", "status": "200",'

"""
CC_TIMESTAMP_STRPTIME = "%Y%m%d%H%M%S"
# sa_session = make_session(engine)
Base = get_declarative_base()
tz_utz = timezone('UTC')


# create Clickhouse SQLAlchemy engine
def make_engine(echo=False, pool_size=100, max_overflow=100):
    return create_engine(url=CH_CONNECT_URI,
                         echo=False,
                         pool_size=pool_size,
                         max_overflow=max_overflow)

# generate Clickhouse SQLAlchemy session
def sa_session(engine=None, echo=False):
    engine = engine or make_engine(echo=echo)
    return make_session(engine)


BACKOFF_EXCEPTIONS = (RequestException, HTTPError, Timeout, ConnectionError,
                      RuntimeError)
BACKOFF_MAX_TIME = 60
BACKOFF_MAX_TRIES = 25


def ensure(d: dict, keys: Union[str, Iterable[str]], default=None):
    if isinstance(keys, str):
        keys = [keys]
    for key in keys:
        if key not in d:
            d[key] = default


def backoff_reporting(kind: str, details: dict):
    ensure(details, 'kind', kind)
    ensure(details, ('elapsed', 'exception', 'tries', 'value', 'wait', 'target'))
    logger.warning("{kind} {wait}s after {tries} tries "
                   "due to {exception}; func {target}, args: {args},"
                   "kwargs: {kwargs}, elapsed {elapsed}"
                   "wait: {wait}, value: {value}".format(**details))

on_backoff_rpt = partial(backoff_reporting, 'Backing off')
on_success_rpt = partial(backoff_reporting, 'Success')
on_giveup_rpt = partial(backoff_reporting, 'Gave up')