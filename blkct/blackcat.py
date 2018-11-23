# -*- coding:utf-8 -*-
from __future__ import annotations

import asyncio
import enum
import time
from typing import cast, Any, Awaitable, Callable, Dict, Generic, List, Optional, TypeVar

import aiohttp
from aiohttp import CookieJar
import asyncpool

from blkct import ClientResponse
from .exceptions import BadStatusCode, URLAlreadyInQueue
from .logging import logger
from .typing import URL


USER_AGENT = 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; .NET CLR 1.1.4322; InfoPath.1; .NET CLR 2.0.50727; '
'.NET CLR 3.0.04425.00)'


class CrawlPrioirty(enum.IntEnum):
    default = 1000
    high = 500
    low = 2000
    image = 5000


PlanCallable = Callable[..., Awaitable[Any]]
TParserResult = TypeVar('TParserResult')
ParserCallable = Callable[[URL, ClientResponse, bytes], TParserResult]


class Terminator:
    pass


# parserによって、T型は違うんだからクラスレベルでTParserResultとかするのはまずいのでは
class Blackcat(Generic[TParserResult]):
    aio_session: Optional[aiohttp.ClientSession]
    last_request: Optional[float]
    task_pool: asyncpool.PriorityAsyncPool
    queued_urls: Dict[URL, bool]

    def __init__(self, num_workers: int = 1, user_agent: Optional[str] = None):
        self.aio_session = None
        self.num_workers = num_workers
        self.queued_urls = {}
        self.last_request = None
        self.request_interval = 2  # 最低リクエスト間隔
        self.user_agent = user_agent or USER_AGENT
        self.task_pool = asyncpool.PriorityAsyncPool(
            None, self.num_workers, 'Blackcat', logger, self.crawl_url, load_factor=0, return_futures=True
        )

    async def crawl(self, url: URL, parser: ParserCallable, priority: CrawlPrioirty = CrawlPrioirty.default,
                    check_status: bool = True) -> TParserResult:
        """
        クロールしたいURLとそれに対応するパーザを登録する。 parse結果が返される
        """
        assert isinstance(url, str)
        assert self.task_pool is not None

        if url in self.queued_urls:
            logger.warning('`%s` is already in queue', url)
            raise URLAlreadyInQueue

        logger.info('put %s (queue: %d / %d)', url, len(self.queued_urls), self.task_pool._queue.qsize())
        self.queued_urls[url] = True

        future = await self.task_pool.push_with_priority(priority, url, parser, check_status=check_status)
        assert future is not None
        return cast(TParserResult, await future)

    async def run(self, plan: PlanCallable, **plan_args: List[Any]) -> None:
        assert self.task_pool.is_empty()
        assert self.aio_session is None
        self.aio_session = self.make_aio_session()
        assert self.aio_session is not None

        async with self.aio_session, self.task_pool:
            await plan(self, **plan_args)

        self.aio_session.close()

    async def crawl_url(self, url: URL, parser: ParserCallable, check_status: bool) -> TParserResult:
        """
        task poolから呼び出されるようのメソッド
        """
        assert self.aio_session is not None
        logger.info('crawl_url %s', url)

        now = time.time()
        if self.last_request and self.last_request + self.request_interval > now:
            wait = (self.last_request + self.request_interval) - now
            logger.debug('sleep %f secs', wait)
            await asyncio.sleep(wait)

        self.queued_urls.pop(url)
        self.last_request = time.time()
        async with self.aio_session.get(url) as resp:
            if check_status:
                if resp.status != 200:
                    raise BadStatusCode(resp.status)

            return cast(TParserResult, parser(url, resp, await resp.read()))

    def make_aio_session(self) -> aiohttp.ClientSession:
        """aiohttp.ClientSessionを作って返す"""
        session = aiohttp.ClientSession(cookie_jar=CookieJar(), headers={'User-Agent': self.user_agent})  # type: ignore
        return cast(aiohttp.ClientSession, session)
