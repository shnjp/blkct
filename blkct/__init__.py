from __future__ import annotations

from typing import Callable, TYPE_CHECKING, TypeVar

from yarl import URL

from .constants import CrawlPrioirty
from .globals import current_blackcat
# from .blackcat import Blackcat, CrawlPrioirty
# from .exceptions import *  # noqa

if TYPE_CHECKING:
    from typing import Optional

__all__ = (
    'CrawlPrioirty',
    #     'Blackcat',
    'URL',
    #     'CrawlerError',
    #     'URLAlreadyInQueue',
    #     'BadStatusCode',
    #     'ParserError',
)

# return type for decorated function
RT = TypeVar('RT')


def register_planner(name: Optional[str] = None) -> Callable[[Callable[..., RT]], Callable[..., RT]]:

    def decorator(f: Callable[..., RT]) -> Callable[..., RT]:
        current_blackcat.register_planner(f, name)
        return f

    return decorator


def register_content_parser(url_pattern: str, flags: int = 0) -> Callable[[Callable[..., RT]], Callable[..., RT]]:

    def decorator(f: Callable[..., RT]) -> Callable[..., RT]:
        current_blackcat.register_content_parser(url_pattern, flags, f)
        return f

    return decorator
