from __future__ import annotations

import io
from typing import Any, Optional, TYPE_CHECKING

import boto3

from botocore.exceptions import ClientError

from .content import FetchedContent, StoredContent, url_to_path
from ..typing import ContentStore

if TYPE_CHECKING:
    from yarl import URL

    from ..session import BlackcatSession


class S3ContentStore(ContentStore):
    """s3に貯めるストア"""

    bucket: Any

    def __init__(self, bucket: str, key_prefix: str):
        self.bucket = bucket
        self.key_prefix = key_prefix

    # override
    async def pull_content(self, session: BlackcatSession, url: URL) -> Optional[StoredContent]:
        # TODO:blockしている
        obj = self.get_obj(session, url)

        try:
            with io.BytesIO() as fp:
                obj.download_fileobj(fp)

                return StoredContent(obj.content_type, fp.getvalue())
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "404":
                # no content
                return None
            raise

    async def push_content(self, session: BlackcatSession, url: URL, content: FetchedContent) -> None:
        # TODO:blockしている
        obj = self.get_obj(session, url)

        with io.BytesIO(content.body) as fp:
            obj.upload_fileobj(fp, {"ContentType": content.content_type})

    # private
    def make_key(self, session: BlackcatSession, url: URL) -> str:
        return self.key_prefix + url_to_path(session.session_id, url)

    def get_obj(self, session: BlackcatSession, url: URL) -> Any:
        return boto3.resource("s3").Object(self.bucket, self.make_key(session, url))
