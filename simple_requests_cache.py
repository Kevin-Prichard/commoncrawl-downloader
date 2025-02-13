import hashlib
import os
import re

import backoff
import requests

from common import (BACKOFF_EXCEPTIONS, BACKOFF_MAX_TIME, BACKOFF_MAX_TRIES)
from common import (on_backoff_rpt, on_success_rpt, on_giveup_rpt)


class SimpleRequestsCache:
    def __init__(self, folder: str, url: str, *args, **kwargs):
        self.folder = folder
        url_hash = hashlib.md5(url.encode()).hexdigest()
        leaf = re.sub(r"[^a-zA-Z0-9_\-.]", "_", url)
        cache_path = os.path.join(self.folder, f"{leaf}-{url_hash}.cache")
        self.response = None
        self.read_file = None
        self.write_file = None
        if os.path.exists(cache_path):
            self.read_file = open(cache_path, 'rb')
        else:
            self.response = self._requests_get(url, *args, **kwargs)
            self.write_file = open(cache_path, 'wb')
        if self.read_file is None and self.write_file is None:
            import pudb; pu.db

    @backoff.on_exception(backoff.expo, BACKOFF_EXCEPTIONS,
                          max_time=BACKOFF_MAX_TIME,
                          max_tries=BACKOFF_MAX_TRIES,
                          on_success=on_success_rpt,
                          on_backoff=on_backoff_rpt,
                          on_giveup=on_giveup_rpt)
    def _requests_get(self, url, *args, **kwargs):
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

    def read(self, size=-1):
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
        if self.response:
            return self.response.raw.tell()
        return self.read_file.tell()

    def length(self):
        if self.response:
            return int(self.response.headers.get('content-length'))
        return os.stat(self.read_file.name).st_size

    def __iter__(self):
        if self.response:
            return iter(self.response.raw)
        return iter(self.read_file)
