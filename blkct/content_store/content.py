from __future__ import annotations

import abc
import os
from typing import Any, Optional

from bs4 import BeautifulSoup

from multidict import CIMultiDictProxy

from yarl import URL

DEFAULT_MIME_TYPE = "application/octet-stream"


class Content(metaclass=abc.ABCMeta):
    @property
    def body(self) -> bytes:
        raise NotImplementedError

    @property
    def content_type(self) -> str:
        raise NotImplementedError

    @property
    def parsed_html(self) -> Any:
        if self.content_type != "text/html":
            raise ValueError("content is not html")

        return BeautifulSoup(self.body, "html.parser")


class StoredContent(Content):
    """Content Storeから取ってきたContent"""

    def __init__(self, content_type: Optional[str], body: bytes):
        self._content_type = content_type or DEFAULT_MIME_TYPE
        self._body = body

    @property
    def body(self) -> bytes:
        return self._body

    @property
    def content_type(self) -> str:
        return self._content_type


class FetchedContent(Content):
    """Content Storeから取ってきたContent"""

    def __init__(self, status_code: int, headers: CIMultiDictProxy, body: bytes):
        self._status_code = status_code
        self._headers = headers
        self._body = body

    @property
    def status_code(self) -> int:
        return self._status_code

    @property
    def body(self) -> bytes:
        return self._body

    @property
    def content_type(self) -> str:
        return self._headers.get("Content-Type", DEFAULT_MIME_TYPE)


# utility
def url_to_path(base_dir_path: str, url: URL, extension: Optional[str] = None) -> str:
    if url.scheme not in ("http", "https") or not url.host or not url.port:
        raise ValueError("url not supported")
    if url.fragment:
        raise ValueError("url has fragment")

    assert url.raw_path_qs.startswith("/")
    filepath = (
        url.raw_path_qs[1:].replace("_", "%5f").replace("/", "__").replace("?", "@@")
    )
    if extension:
        filepath += extension

    return os.path.join(base_dir_path, f"{url.scheme}:{url.host}:{url.port}", filepath)
