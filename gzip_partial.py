import gzip
from io import BytesIO
import sys
from typing import Generator
import zlib

import backoff
from requests.sessions import Session

from common_requests import HTTP_BACKOFF_EXCEPTIONS, BACKOFF_MAX_TIME, \
    BACKOFF_MAX_TRIES, on_backoff_rpt, on_success_rpt, on_giveup_rpt


def gzip_decompress_partial(data: bytes, max_length: int=sys.maxsize):
    """Decompress part of a gzip compressed string.
    Return the decompressed string, ignoring CRC32 which we cannot validate due
    to potentially not having the entire file (thanks to param max_length).

    The bones of this were swiped from gzip.py in the standard library, because
    it doesn't expose the max_length parameter of zlib.decompressobj, which we
    need to in order to inspect the beginning of gzipped HTTP resources w/o
    downloading them in entirety.
    """
    decompressed_members = []
    while True:
        fp = BytesIO(data)

        # Test gzip stream header for correctness; return if no further blocks
        if gzip._read_gzip_header(fp) is None:
            return b"".join(decompressed_members)

        # Use a zlib raw deflate compressor
        do = zlib.decompressobj(wbits=-zlib.MAX_WBITS)

        # Read all the data except the header
        decompressed = do.decompress(
            data[fp.tell():], max_length=max_length or len(data)
        )
        decompressed_members.append(decompressed)
        data = do.unused_data[8:].lstrip(b"\x00")


@backoff.on_exception(backoff.expo, HTTP_BACKOFF_EXCEPTIONS,
                      max_time=BACKOFF_MAX_TIME,
                      max_tries=BACKOFF_MAX_TRIES,
                      on_success=on_success_rpt,
                      on_backoff=on_backoff_rpt,
                      on_giveup=on_giveup_rpt)
def get_gz_resource(url: str,
                    user_agent: str,
                    requests_session: Session,
                    length: int=None,
                    max_uncompressed_length: int=sys.maxsize):
    """Fetch a gzipped resource from the web, and decompress it partfially.

    Args:
        url:str - The URL of the resource to fetch
        user_agent:str - The User-Agent string to use in the request
        requests_session: Session (CachedSession)
        length:int - The number of decompressed bytes to fetch from the resource
        max_uncompressed_length:int - max compressed bytes to fetch and decompress
    Returns:
        str: The decompressed content at url, as utf-8 string
    """

    headers = {
        'user-agent': user_agent,
        'accept-encoding': 'gzip',
    }
    if length:
        headers['Range'] = f"bytes=0-{length}"
    response = requests_session.get(url, headers=headers)

    if response.status_code > 299:
        raise RuntimeError(
            f"Failed to fetch index file {url}: {response.status_code}")

    content = gzip_decompress_partial(
        response.content,
        max_length=max_uncompressed_length
    ).decode('utf-8')
    del response
    return content


def gzip_decompress_stream(stream: BytesIO,
                           stream_len: int,
                           buf_size: int) -> Generator[bytes, None, None]:

    o = zlib.decompressobj(16 + zlib.MAX_WBITS)

    countdown = stream_len
    countup = 0
    buf = bytearray(buf_size)
    stream_empty = False
    while (not stream_empty and
           (readlen := stream.readinto(buf))):
        countup += readlen
        countdown -= readlen
        try:
            # See https://is.gd/xxUGIM
            decomp = o.decompress(buf)
            yield decomp, countdown
        except Exception as ee:
            from traceback import print_exc
            print_exc()
            print(f"Exception: {ee}")

        if readlen < buf_size < countdown:
            print(f"Hmmm... {readlen} < {buf_size} < {countdown}")
        stream_empty = countdown <= 0

        del buf
        buf = bytearray(buf_size)

    yield o.flush()
