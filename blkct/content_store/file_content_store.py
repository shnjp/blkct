from __future__ import annotations

import glob
import mimetypes
import os
from typing import Optional, TYPE_CHECKING

from .content import FetchedContent, StoredContent
from ..logging import logger
from ..typing import ContentStore

if TYPE_CHECKING:
    from yarl import URL

    from ..session import BlackcatSession


class FileContentStore(ContentStore):
    """fileに貯めるストア"""

    store_root_path: str

    def __init__(self, store_root_path: str):
        self.store_root_path = store_root_path

    async def pull_content(self, session: BlackcatSession, url: URL) -> Optional[StoredContent]:
        filepathpattern = url_to_path(os.path.join(self.store_root_path, session.session_id), url) + '*'
        files = glob.glob(filepathpattern)

        if not files:
            return None

        if len(files) > 1:
            logger.warning('multiple content file found %r', files)

        filepath = files[0]
        dirpath, filename = os.path.split(filepath)
        content_type, encoding = mimetypes.guess_type(filename)

        with open(filepath, 'rb') as fp:
            return StoredContent(content_type, fp.read())

    async def push_content(self, session: BlackcatSession, url: URL, content: FetchedContent) -> None:
        ext = mimetypes.guess_extension(content.content_type)
        filepath = url_to_path(os.path.join(self.store_root_path, session.session_id), url, ext)
        dirpath, filename = os.path.split(filepath)
        logger.info('save %s content to %s', url, filepath)

        # prepare directory
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)

        with open(filepath, 'wb') as fp:
            fp.write(content.body)


def url_to_path(base_dir_path: str, url: URL, extension: Optional[str] = None) -> str:
    if url.scheme not in ('http', 'https') or not url.host or not url.port:
        raise ValueError('url not supported')
    if url.fragment:
        raise ValueError('url has fragment')

    assert url.raw_path_qs.startswith('/')
    filepath = (url.raw_path_qs[1:].replace('_', '%5f').replace('/', '__').replace('?', '@@'))
    if extension:
        filepath += f'.{extension}'

    return os.path.join(base_dir_path, f'{url.scheme}:{url.host}:{url.port}', filepath)
