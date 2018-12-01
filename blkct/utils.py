import datetime
import json
import os
from typing import Any

from yarl import URL


def make_new_session_id() -> str:
    """SessionIDのためのUniqueっぽい文字列を作る"""
    return f'{datetime.datetime.utcnow():%Y%m%d%H%M%S}-{os.urandom(4).hex().upper()}'


def dump_json(obj: Any) -> str:
    return BlkctEncoder().encode(obj)


class BlkctEncoder(json.JSONEncoder):

    def default(self, obj: Any) -> Any:
        if isinstance(obj, URL):
            return str(obj)

        return super().default(obj)
