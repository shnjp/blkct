import datetime
import os


def make_new_session_id() -> str:
    """SessionIDのためのUniqueっぽい文字列を作る"""
    return f'{datetime.datetime.utcnow():%Y%m%d%H%M%S}-{os.urandom(4).hex().upper()}'
