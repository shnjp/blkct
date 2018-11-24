from __future__ import annotations

import abc
from typing import Any, Awaitable, Callable, Dict, Optional

from yarl import URL

from .content_store.content import Content, FetchedContent, StoredContent
from .session import BlackcatSession

__all__ = ('ContentParserType', 'PlannerType', 'Scheduler', 'URL', 'SchedulerFactory', 'ContentStoreFactory')

ContentParserType = Callable[[URL, Dict[str, str], Content], Any]
PlannerType = Callable[..., Awaitable[Any]]


class Scheduler(metaclass=abc.ABCMeta):
    """
    plannerのスケジューリングをする
    """

    @abc.abstractmethod
    async def dispatch(self, session: BlackcatSession, planner: str, args: Dict[str, str]) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def run(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def run_once(self) -> None:
        raise NotImplementedError


class ContentStore(metaclass=abc.ABCMeta):
    """
    plannerのスケジューリングをする
    """

    @abc.abstractmethod
    async def pull_content(self, session: BlackcatSession, url: URL) -> Optional[StoredContent]:
        raise NotImplementedError

    @abc.abstractmethod
    async def push_content(self, session: BlackcatSession, url: URL, content: FetchedContent) -> None:
        raise NotImplementedError


SchedulerFactory = Callable[[], Scheduler]
ContentStoreFactory = Callable[[], ContentStore]
