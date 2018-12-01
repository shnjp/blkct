from __future__ import annotations

from typing import Any, Mapping, TYPE_CHECKING, cast

import boto3

from ..exceptions import ContextNotFoundError
from ..typing import ContextStore

if TYPE_CHECKING:
    from ..session import BlackcatSession


class DynamoDBContextStore(ContextStore):
    table: Any

    def __init__(self, table: str):
        self.table = boto3.resource('dynamodb').Table(table)

    async def close(self) -> None:
        pass

    async def load(self, session: BlackcatSession, key: str) -> Mapping[str, Any]:
        rv = self.table.get_item(Key={'key': key})
        assert rv['ResponseMetadata']['HTTPStatusCode'] == 200
        if 'Item' not in rv:
            raise ContextNotFoundError()

        assert rv['Item']['key'] == key

        return cast(Mapping[str, Any], rv['Item']['payload'])

    async def save(self, session: BlackcatSession, key: str, data: Mapping[str, Any]) -> None:
        rv = self.table.put_item(Item={'key': key, 'payload': dict(data)})
        if rv['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception('DynamoDB put failed %r', rv)
