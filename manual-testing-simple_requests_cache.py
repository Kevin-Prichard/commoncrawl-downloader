#!/usr/bin/env python3

import hashlib
import pudb
from simple_requests_cache import SimpleRequestsCache
import sys

test_url = "http://localhost/cdx-00129.urls-kp"  # CC-MAIN-20241201162023-20241201192023-00024.warc.gz"
folder = "/tmp"
block_size = 2**20 * 100
force_rewrite=True


def main(folder, test_url, block_size, force_rewrite=False):
    hasher = hashlib.md5()
    src = SimpleRequestsCache(folder=folder,
                              url=test_url,
                              block_size=block_size,
                              stream=True)
    block_nr = 0
    bytes_total = 0
    while block := src.read(block_size):
        bytes_total += len(block)
        print(f"===> block {block_nr}, bytes so far: {bytes_total}")
        hasher.update(block)
        block_nr += 1
    print(hasher.hexdigest())
    src.close()


if __name__ == "__main__":
    s = sys.argv[1:]
    if len(s) == 3:
        folder, test_url, block_size = s
    else:
        folder, test_url, block_size, force_rewrite = (
            folder, test_url, block_size, force_rewrite)
    main(folder, test_url, block_size, force_rewrite=True)
