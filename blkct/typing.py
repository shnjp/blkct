from __future__ import annotations

import abc
from typing import Any, Awaitable, Callable, Dict, Mapping, NamedTuple, Optional

from yarl import URL

from .content_store.content import Content, FetchedContent, StoredContent
from .session import BlackcatSession

__all__ = (
    'ContentParserType', 'PlannerType', 'Scheduler', 'URL', 'SchedulerFactory', 'ContentStoreFactory', 'ContextStore',
    'ContextStoreFactory'
)

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


class ContextStore(metaclass=abc.ABCMeta):
    """
    plannerの状態を保存する、ContextStore
    """

    @abc.abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def load(self, session: BlackcatSession, key: str) -> Mapping[str, Any]:
        raise NotImplementedError

    @abc.abstractmethod
    async def save(self, session: BlackcatSession, key: str, data: Mapping[str, Any]) -> None:
        raise NotImplementedError


SchedulerFactory = Callable[[], Scheduler]
ContentStoreFactory = Callable[[], ContentStore]
ContextStoreFactory = Callable[[], ContextStore]


class BinaryData(NamedTuple):
    url: URL
    content_type: str
    body: bytes
