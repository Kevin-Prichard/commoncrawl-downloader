from functools import partial
import hashlib
import multiprocessing as mp
import logging
from multiprocessing import Process
import os
from os import stat
import re
import time
import traceback as tb

import backoff
import requests

from common_requests import (HTTP_BACKOFF_EXCEPTIONS, BACKOFF_MAX_TIME,
                             BACKOFF_MAX_TRIES, ensure)


logger = logging.getLogger(__name__)


def read_file_open_rpt(kind: str, details: dict):
    if kind == "Read file success":
        return
    ensure(details, 'kind', kind)
    ensure(details, ('elapsed', 'exception', 'tries',
                     'value', 'wait', 'target'))
    logger.warning(
        "{kind} {wait}s after {tries} tries "
        "due to {exception}; func {target}, args: {args},"
        "kwargs: {kwargs}, elapsed {elapsed}"
        "wait: {wait}, value: {value}".format(**details))
    tb.print_last()


on_read_backoff = partial(read_file_open_rpt, "Read file BACKOFF")
on_read_success = partial(read_file_open_rpt, "Read file success")
on_read_giveup = partial(read_file_open_rpt, "Read file GIVEUP")


class SimpleRequestsCache:
    def __init__(self, folder: str, url: str, block_size: int = 2**16,
                 force_rewrite=False,
                 *args, **kwargs):
        self.folder = folder
        self.url = url
        self.block_size = block_size
        url_hash = hashlib.md5(url.encode()).hexdigest()
        leaf = re.sub(r"[^a-zA-Z0-9_\-.]", "_", url)
        self.cache_path = os.path.join(self.folder, f"{leaf}-{url_hash}.cache")
        self.response = None
        self.read_file = None
        self.write_file = None
        self.cache_process = None
        self._content_length = None
        cache_exists = self.check_cache_sanity()
        if force_rewrite or not cache_exists:
            msg = "{'does not exist' if not cache_exists else 'force_rewrite' if force_rewrite else 'exists'}"
            print(f"Cache {msg}: {self.cache_path}")
            self.cache_process = Process(target=self.read_write_loop,
                                         args=args, kwargs=kwargs)
            t = self.cache_process.start()
            print(t)
            time.sleep(1)
        self.read_file = self.open_read_file()

        # if self.read_file is None and self.write_file is None:
        #     import pudb; pu.db

    @backoff.on_exception(backoff.expo, HTTP_BACKOFF_EXCEPTIONS,
                          max_time=BACKOFF_MAX_TIME,
                          max_tries=1000,
                          on_success=on_read_success,
                          on_backoff=on_read_backoff,
                          on_giveup=on_read_giveup)
    def check_cache_sanity(self):
        pu.db
        if exists := os.path.exists(self.cache_path):
            cache_file_size = os.stat(self.cache_path).st_size
            if cache_file_size == 0:
                do_remove = True
            else:
                head = requests.head(self.url)
                do_remove = (int(head.headers.get("Content-Length", -1))
                             > cache_file_size)
            if do_remove:
                os.remove(self.cache_path)
                return True
        return False

    @backoff.on_exception(backoff.expo, HTTP_BACKOFF_EXCEPTIONS,
                          max_time=BACKOFF_MAX_TIME,
                          max_tries=1000,
                          on_success=on_read_success,
                          on_backoff=on_read_backoff,
                          on_giveup=on_read_giveup)
    def open_read_file(self):
        exists = os.path.exists(self.cache_path)
        print(f"Read file [{'EXISTS' if exists else 'DOES NOT EXIST'}]: "
              f"{self.cache_path}")
        return open(self.cache_path, 'rb')

    @property
    def content_length(self):
        return self._content_length or self.length()

    def read_write_loop(self, *args, **kwargs):
        print(f"Read write loop: {(self.url, args, kwargs)}")
        self.response = self._requests_get(self.url, *args, **kwargs)
        print(f"Response: {self.response}")
        self._content_length = self.response.headers.get("Content-Length", -1)
        print(f"Content-Length: {self.content_length}")
        self.write_file = open(self.cache_path, 'wb')
        print(f"Write file: {self.write_file}")
        bytes_total = 0
        print(f"About to read "
              f"{self.response}, "
              f"{self.response.raw}, "
              f"{self.response.raw.read}")

        while chunk := self.response.raw.read(self.block_size):
            # print(f"Chunk: {len(chunk)}")
            bytes_written = self.write_file.write(chunk)
            self.write_file.flush()
            bytes_total += bytes_written
            print(f"Bytes written: {bytes_written} / {bytes_total}")

        self.write_file.close()
        self.response.close()
        self.response = None
        # self.cache_process.terminate()
        return bytes_total, self.content_length

    @backoff.on_exception(backoff.expo, HTTP_BACKOFF_EXCEPTIONS,
                          max_time=BACKOFF_MAX_TIME,
                          max_tries=BACKOFF_MAX_TRIES,
                          on_success=on_read_success,
                          on_backoff=on_read_backoff,
                          on_giveup=on_read_giveup)
    def _requests_get(self, url, *args, **kwargs) -> requests.Response:
        return requests.get(url, *args, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        if self.write_file:
            self.write_file.close()
        if self.read_file:
            self.read_file.close()
        if self.response:
            self.response.close()

    close = __exit__
    __del__ = __exit__

    @backoff.on_exception(backoff.expo, (Exception,),
                          max_time=BACKOFF_MAX_TIME,
                          max_tries=BACKOFF_MAX_TRIES,
                          on_success=on_read_success,
                          on_backoff=on_read_backoff,
                          on_giveup=on_read_giveup)
    def read(self, size=-1):
        print(f"SimpleRequestsCache.read() wants {size}, "
              f"have {self.content_length - self.tell()} ")
        if self.response:
            chunk = self.response.raw.read(size)
            self.write_file.write(chunk)
            return chunk
        return self.read_file.read(size)

    def readline(self, size=-1):
        if self.response:
            line = self.response.raw.readline(size)
            self.write_file.write(line)
            return line
        return self.read_file.readline(size)

    def readlines(self, hint=-1):
        if self.response:
            lines = self.response.raw.readlines(hint)
            self.write_file.writelines(lines)
            return lines
        return self.read_file.readlines(hint)

    def seek(self, offset, whence=0):
        raise NotImplementedError("seek not supported")
        # if self.response:
        #     return self.response.raw.seek(offset, whence)
        # return self.read_file.seek(offset, whence)

    def tell(self):
        # if self.response:
        #     return self.response.raw.tell()
        return self.read_file.tell()
        # read stat and tell dif different

    def length(self):
        return self._content_length or os.stat(self.read_file.name).st_size
        # if self.response:
        #     return int(self.response.headers.get('content-length'))
        # return os.stat(self.read_file.name).st_size

    def __iter__(self):
        if self.response:
            return iter(self.response.raw)
        return iter(self.read_file)
