from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, cast

import aiohttp

from yarl import URL

from .logging import logger
from .session import BlackcatSession
from .setup import ContentParserEntry

if TYPE_CHECKING:
    from types import TracebackType
    from typing import Any, AsyncGenerator, Dict, List, Mapping, Optional, Tuple, Type

    from .typing import (
        ContentParserType,
        ContentStoreFactory,
        ContextStoreFactory,
        PlannerType,
        SchedulerFactory,
    )


class Blackcat:
    content_parsers: List[ContentParserEntry]
    # content_store_factory: ContentStoreFactory
    planners: Dict[str, PlannerType]
    # scheduler_factory: SchedulerFactory
    session: Optional[BlackcatSession]

    def __init__(
        self,
        planners: Dict[str, PlannerType],
        content_parsers: List[ContentParserEntry],
        scheduler_factory: SchedulerFactory,
        content_store_factory: ContentStoreFactory,
        context_store_factory: ContextStoreFactory,
        user_agent: Optional[str] = None,
    ):
        self.planners = planners
        self.content_parsers = content_parsers
        self.scheduler_factory = scheduler_factory
        self.content_store_factory = content_store_factory
        self.context_store_factory = context_store_factory
        self.session = None
        self.user_agent = user_agent or "blkct crawler"
        self.request_interval = 5.0

    # public
    @contextlib.asynccontextmanager
    async def start_session(
        self, session_id: str
    ) -> AsyncGenerator[BlackcatSession, None]:
        if self.session:
            raise ValueError("session is already started")
        self.session = BlackcatSession(
            self,
            self.scheduler_factory(),
            self.content_store_factory(),
            self.context_store_factory(),
            session_id=session_id,
        )
        logger.info("Start session(id=%s)", self.session.session_id)
        try:
            yield self.session
        finally:
            if self.session:
                await self.session.close()
            self.session = None

    async def run_with_session(
        self, planner: str, args: Mapping[str, Any], session_id: str
    ) -> None:
        async with self.start_session(session_id=session_id) as session:
            await session.dispatch(planner, args)
            await session.scheduler.run()

    # internal
    def get_content_parsers_by_url(
        self, url: URL
    ) -> Tuple[ContentParserType, Dict[str, str]]:
        if url.scheme not in ("http", "https"):
            raise ValueError(f"Bad URL `{url}`")

        found = []
        for pattern, parser in self.content_parsers:
            mo = pattern.match(str(url))
            if mo:
                found.append((mo, parser))

        if len(found) > 2:
            raise Exception(f"Multiple parser found for url `{url}`")
        elif not found:
            raise Exception(f"No parser found for url `{url}`")
        mo, parser = found[0]
        return parser, mo.groupdict()

    def make_aio_session(self) -> aiohttp.ClientSession:
        """aiohttp.ClientSessionを作って返す"""
        session = aiohttp.ClientSession(  # type: ignore
            cookie_jar=aiohttp.CookieJar(), headers={"User-Agent": self.user_agent}
        )
        return cast(aiohttp.ClientSession, session)


def reraise(
    exc_type: Type[BaseException],
    exc_value: Exception,
    tb: Optional[TracebackType] = None,
) -> None:
    if exc_value.__traceback__ is not tb:
        raise exc_value.with_traceback(tb)
    raise exc_value
