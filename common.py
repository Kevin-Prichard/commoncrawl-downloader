from collections import namedtuple
import logging

from pytz import timezone
import re

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, Session
from config import DB_CONNECT_URI

logger = logging.getLogger(__name__)


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
    rows. The headers are recorded in CdxFirstUrl table but not used anywhere. """


CC_TIMESTAMP_STRPTIME = "%Y%m%d%H%M%S"
Base = declarative_base()
tz_utz = timezone('UTC')

engine = None

# create SQLAlchemy engine
def make_engine(echo=False, pool_size=100, max_overflow=100):
    global engine
    return create_engine(url=DB_CONNECT_URI,
                         echo=False,
                         pool_size=pool_size,
                         max_overflow=max_overflow)

# generate SQLAlchemy session
def sa_session(echo=False):
    global engine
    engine = make_engine(echo=echo)
    return Session(engine)
