from __future__ import annotations

import abc
from typing import Any

from bs4 import BeautifulSoup

from multidict import CIMultiDictProxy


class Content(metaclass=abc.ABCMeta):

    @property
    def content(self) -> bytes:
        raise NotImplementedError

    @property
    def content_type(self) -> str:
        raise NotImplementedError

    @property
    def parsed_html(self) -> Any:
        if self.content_type != 'text/html':
            raise ValueError('content is not html')

        return BeautifulSoup(self.content, 'html.parser')


class StoredContent(Content):
    """Content Storeから取ってきたContent"""
    pass


class FetchedContent(Content):
    """Content Storeから取ってきたContent"""

    def __init__(self, status_code: int, headers: CIMultiDictProxy, content: bytes):
        self._status_code = status_code
        self._headers = headers
        self._content = content

    @property
    def content(self) -> bytes:
        return self._content

    @property
    def content_type(self) -> str:
        return self._headers.get('Content-Type', 'application/octet-stream')
