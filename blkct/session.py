from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

from yarl import URL

from .exceptions import BadStatusCode
from .logging import logger

if TYPE_CHECKING:
    from typing import Any, Dict, Optional, Tuple, Union

    import aiohttp

    from .blackcat import Blackcat
    from .typing import ContentParserType, Scheduler


class BlackcatSession:
    aio_session: aiohttp.ClientSession
    blackcat: Blackcat
    last_request_time_per_host: Dict[Tuple[str, int], float]
    scheduler: Scheduler
    session_id: str

    def __init__(self, blackcat: Blackcat, scheduler: Scheduler, session_id: str):
        self.blackcat = blackcat
        self.scheduler = scheduler
        self.aio_session = self.blackcat.make_aio_session()
        self.last_request_time_per_host = {}
        self.session_id = session_id

    # public
    async def crawl(
        self, url: Union[str, URL], *, parser: Optional[ContentParserType] = None, check_status: bool = True
    ) -> Any:
        url = URL(url) if isinstance(url, str) else url
        if url.scheme not in ('http', 'https') or not url.host or not url.port:
            raise ValueError(f'Bad URL `{url}`')

        # check if parser exists
        if not parser:
            parser, params = self.blackcat.get_content_parsers_by_url(url)
            if not parser:
                raise Exception(f'No parsers found for URL `{url}`')
        else:
            params = {}

        logger.info('Crawl URL: %s', url)

        # ホストにアクセスする間隔についてwaitを入れる
        now = time.time()
        host_key = (url.host, url.port)
        last_request = self.last_request_time_per_host.get(host_key, None)
        if last_request:
            wait_until = last_request + self.blackcat.request_interval
            if wait_until > now:
                logger.debug('sleep %f secs', wait_until - now)
                await asyncio.sleep(wait_until - now)

        self.last_request_time_per_host[host_key] = now

        # crawl and parse
        async with self.aio_session.get(url) as resp:
            if check_status:
                if resp.status != 200:
                    raise BadStatusCode(resp.status)

            return parser(url, params, resp, await resp.read())

    async def dispatch(self, planner: str, **args: Any) -> None:
        logger.info('Dispatch %s with args %r', planner, args)
        await self.scheduler.dispatch(self, planner, args)

    # internal
    async def close(self) -> None:
        await self.aio_session.close()

    # private
    async def handle_planner(self, planner: str, args: Dict[str, Any]) -> Any:
        logger.info('Handle planner %s with args %r', planner, args)

        # TODO: check exists
        p = self.blackcat.planners[planner]
        return await p(self, **args)
