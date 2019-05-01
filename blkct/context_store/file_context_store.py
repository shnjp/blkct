from __future__ import annotations

import dbm
import json
from typing import Any, Mapping, TYPE_CHECKING, cast

from ..exceptions import ContextNotFoundError
from ..typing import ContextStore

if TYPE_CHECKING:
    from ..session import BlackcatSession

SENTINEL = object()


class FileContextStore(ContextStore):
    db_file_path: str
    db: Any

    def __init__(self, db_file_path: str):
        self.db_file_path = db_file_path
        self.db = dbm.open(db_file_path, "c")

    async def close(self) -> None:
        self.db.close()

    async def load(self, session: BlackcatSession, key: str) -> Mapping[str, Any]:
        raw = self.db.get(key, SENTINEL)
        if raw is SENTINEL:
            raise ContextNotFoundError()

        parsed = json.loads(raw)
        return cast(Mapping[str, Any], parsed)

    async def save(
        self, session: BlackcatSession, key: str, data: Mapping[str, Any]
    ) -> None:
        self.db[key] = json.dumps(data)
