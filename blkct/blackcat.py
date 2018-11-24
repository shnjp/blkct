from __future__ import annotations

import contextlib
import re
from typing import NamedTuple, TYPE_CHECKING, cast

import aiohttp

from yarl import URL

from .globals import _setup_ctx_stack
from .session import BlackcatSession

if TYPE_CHECKING:
    from types import TracebackType
    from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Pattern, Tuple, Type, Union

    from .typing import ContentParserType, PlannerType, Scheduler


class ContentParserEntry(NamedTuple):
    pattern: Pattern[str]
    parse: ContentParserType


if TYPE_CHECKING:
    SchedulerFactory = Callable[[], Scheduler]


class Blackcat:
    content_parsers: List[ContentParserEntry]
    planners: Dict[str, PlannerType]
    session: Optional[BlackcatSession]

    def __init__(self, scheduler_factory: SchedulerFactory, user_agent: Optional[str] = None):
        self.planners = {}
        self.content_parsers = []
        self.scheduler_factory = scheduler_factory
        self.session = None
        self.user_agent = user_agent or 'blkct crawler'
        self.request_interval = 5.0

    # public
    def register_planner(self, f: PlannerType, name: Optional[str] = None) -> None:
        if not name:
            name = f.__name__
        if name in self.planners:
            raise ValueError(f'planner `{name}` is already registered.')
        self.planners[name] = f

    def register_content_parser(
        self, url_pattern: Union[str, Pattern[str]], re_flags: re.RegexFlag, f: ContentParserType
    ) -> None:
        pattern = re.compile(url_pattern, re_flags)
        self.content_parsers.append(ContentParserEntry(pattern, f))

    def setup(self) -> 'SetupContext':
        return SetupContext(self)

    @contextlib.asynccontextmanager
    async def start_session(self) -> AsyncGenerator[BlackcatSession, None]:
        if self.session:
            raise ValueError('session is already started')
        self.session = BlackcatSession(self, self.scheduler_factory())
        try:
            yield self.session
        finally:
            if self.session:
                await self.session.close()
            self.session = None

    async def run_with_session(self, planner: str, **args: Any) -> None:
        async with self.start_session() as session:
            await session.dispatch(planner, **args)
            await session.scheduler.run()

    # internal
    def get_content_parsers_by_url(self, url: URL) -> Tuple[ContentParserType, Dict[str, str]]:
        if url.scheme not in ('http', 'https'):
            raise ValueError(f'Bad URL `{url}`')

        found = []
        for pattern, parser in self.content_parsers:
            mo = pattern.match(str(url))
            if mo:
                found.append((mo, parser))

        if len(found) > 2:
            raise Exception(f'Multiple parser found for url `{url}`')
        elif not found:
            raise Exception(f'No parser found for url `{url}`')
        mo, parser = found[0]
        return parser, mo.groupdict()

    def make_aio_session(self) -> aiohttp.ClientSession:
        """aiohttp.ClientSessionを作って返す"""
        session = aiohttp.ClientSession(  # type: ignore
            cookie_jar=aiohttp.CookieJar(), headers={'User-Agent': self.user_agent}
        )
        return cast(aiohttp.ClientSession, session)


class SetupContext:
    blackcat: Blackcat

    def __init__(self, blackcat: Blackcat):
        self.blackcat = blackcat

    def push(self) -> None:
        """Binds the app context to the current context."""
        _setup_ctx_stack.push(self)

    def pop(self, exc: Optional[Exception] = None) -> None:
        """Pops the app context."""
        rv = _setup_ctx_stack.pop()
        assert rv is self, f'Popped wrong app context.  ({rv!r} instead of {self!r})'

    def __enter__(self) -> 'SetupContext':
        self.push()
        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_value: Optional[Exception], tb: Optional[TracebackType]
    ) -> None:
        self.pop(exc_value)

        if exc_type is not None and exc_value is not None:
            reraise(exc_type, exc_value, tb)


def reraise(exc_type: Type[BaseException], exc_value: Exception, tb: Optional[TracebackType] = None) -> None:
    if exc_value.__traceback__ is not tb:
        raise exc_value.with_traceback(tb)
    raise exc_value
