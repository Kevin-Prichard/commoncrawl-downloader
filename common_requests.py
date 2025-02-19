from functools import partial
from typing import Union, Iterable

from requests import RequestException, HTTPError, Timeout, ConnectionError
from requests.sessions import Session

from common import logger
from config import CACHE_REQUESTS

if CACHE_REQUESTS:
    from requests_cache import CachedSession
    requests_session: Session = CachedSession('demo_cache')
else:
    import requests
    requests_session: Session = requests.Session()


HTTP_BACKOFF_EXCEPTIONS = (RequestException, HTTPError, Timeout, ConnectionError,
                           RuntimeError)
BACKOFF_MAX_TIME = 60
BACKOFF_MAX_TRIES = 25


def ensure(d: dict, keys: Union[str, Iterable[str]], default=None):
    if isinstance(keys, str):
        keys = [keys]
    for key in keys:
        if key not in d:
            d[key] = default


RPT_KIND_BACKOFF = 'Backing off'
RPT_KIND_SUCCESS = 'Success'
RPT_KIND_GIVEUP = 'Gave up'


def backoff_reporting(kind: str, details: dict):
    ensure(details, 'kind', kind)
    ensure(details, ('elapsed', 'exception', 'tries',
                     'value', 'wait', 'target'))
    if not (kind == RPT_KIND_SUCCESS and details['tries'] < 2):
        logger.warning(
            "{kind} {wait}s after {tries} tries "
            "due to {exception}; func {target}, args: {args},"
            "kwargs: {kwargs}, elapsed {elapsed}"
            "wait: {wait}, value: {value}".format(**details))


on_backoff_rpt = partial(backoff_reporting, RPT_KIND_BACKOFF)
on_success_rpt = partial(backoff_reporting, RPT_KIND_SUCCESS)
on_giveup_rpt = partial(backoff_reporting, RPT_KIND_GIVEUP)
