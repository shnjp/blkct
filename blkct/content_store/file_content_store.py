from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from .content import StoredContent
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
        return None
