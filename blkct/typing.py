from typing import Any, Awaitable, Callable, Dict

# import aiohttp
from aiohttp.client_reqrep import ClientResponse

from yarl import URL

from .session import BlackcatSession

__all__ = ('ContentParserType', 'PlannerType', 'Scheduler', 'URL')

ContentParserType = Callable[[URL, Dict[str, str], ClientResponse, bytes], Any]
PlannerType = Callable[..., Awaitable[Any]]


class Scheduler:

    async def dispatch(self, session: BlackcatSession, planner: str, args: Dict[str, str]) -> None:
        raise NotImplementedError

    async def run(self) -> None:
        raise NotImplementedError

    async def run_once(self) -> None:
        raise NotImplementedError
