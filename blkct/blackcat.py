import contextlib
import re
from collections import namedtuple
from typing import cast

import aiohttp

from yarl import URL

from .globals import _setup_ctx_stack
from .session import BlackcatSession

# a singleton sentinel value for parameter defaults
_sentinel = object()
ContentParserEntry = namedtuple('ContentParserEntry', ('pattern', 'parser'))


class Blackcat:

    def __init__(self, scheduler_factory, user_agent=None):
        self.planners = {}
        self.content_parsers = []
        self.scheduler_factory = scheduler_factory
        self.session = None
        self.user_agent = user_agent or 'blkct crawler'
        self.request_interval = 5.0

    # public
    def register_planner(self, f, name=None):
        if not name:
            name = f.__name__
        if name in self.planners:
            raise ValueError(f'planner `{name}` is already registered.')
        self.planners[name] = f

    def register_content_parser(self, url_pattern, re_flags, f):
        pattern = re.compile(url_pattern, re_flags)
        self.content_parsers.append(ContentParserEntry(pattern, f))

    def setup(self):
        return SetupContext(self)

    @contextlib.asynccontextmanager
    async def start_session(self):
        if self.session:
            raise ValueError('session is already started')
        self.session = BlackcatSession(self, self.scheduler_factory())
        try:
            yield self.session
        finally:
            await self.session.close()
            self.session = None

    async def run_with_session(self, planner: str, **args):
        async with self.start_session() as session:
            await session.dispatch(planner, **args)
            await session.scheduler.run()

    # internal
    def get_content_parsers_by_url(self, url: URL):
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
        session = aiohttp.ClientSession(
            cookie_jar=aiohttp.CookieJar(), headers={'User-Agent': self.user_agent}
        )  # type: ignore
        return cast(aiohttp.ClientSession, session)


class SetupContext:

    def __init__(self, blackcat):
        self.blackcat = blackcat

    def push(self):
        """Binds the app context to the current context."""
        _setup_ctx_stack.push(self)

    def pop(self, exc=_sentinel):
        """Pops the app context."""
        rv = _setup_ctx_stack.pop()
        assert rv is self, f'Popped wrong app context.  ({rv!r} instead of {self!r})'

    def __enter__(self):
        self.push()
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.pop(exc_value)

        if exc_type is not None:
            reraise(exc_type, exc_value, tb)


def reraise(tp, value, tb=None):
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value
