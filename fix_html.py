#!/usr/bin/env python3

import sys

from lxml import etree


html_parser = etree.HTMLParser(recover=True)


def fix_html(html_file: str) -> str:
    with open(html_file, 'rb') as f:
        html = f.read()
        return etree.tostring(
            etree.fromstring(html, html_parser)).decode('utf-8')


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <html_file>")
        sys.exit(1)

    for html_file in sys.argv[1:]:
        print(fix_html(html_file))


if __name__ == '__main__':
    main()
