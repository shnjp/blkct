from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, TypeVar, cast

from yarl import URL

from .content_store.content import Content, FetchedContent
from .exceptions import BadStatusCode, ContextNotFoundError, CrawlerError
from .logging import logger
from .typing import BinaryData

if TYPE_CHECKING:
    from typing import Any, Dict, Mapping, Optional, Tuple, Union

    import aiohttp

    from .blackcat import Blackcat
    from .typing import ContentParserType, ContentStore, ContextStore, Scheduler


class BlackcatSession:
    aio_session: aiohttp.ClientSession
    blackcat: Blackcat
    content_store: ContentStore
    context_store: ContextStore
    last_request_time_per_host: Dict[Tuple[str, int], float]
    scheduler: Scheduler
    session_id: str

    def __init__(
        self,
        blackcat: Blackcat,
        scheduler: Scheduler,
        content_store: ContentStore,
        context_store: ContextStore,
        session_id: str,
    ):
        self.blackcat = blackcat
        self.content_store = content_store
        self.context_store = context_store
        self.scheduler = scheduler
        self.aio_session = self.blackcat.make_aio_session()
        self.last_request_time_per_host = {}
        self.session_id = session_id

    # public
    async def crawl(
        self, url: Union[str, URL], *, parser: Optional[ContentParserType] = None, check_status: bool = True
    ) -> Any:
        """
        URLをクロールして結果を返す
        """
        url = URL(url) if isinstance(url, str) else url

        # check if parser exists
        if not parser:
            parser, params = self.blackcat.get_content_parsers_by_url(url)
            if not parser:
                raise Exception(f"No parsers found for URL `{url}`")
        else:
            params = {}

        content: Optional[Content] = await self.content_store.pull_content(self, url)
        if content:
            # Content Storeにみつかったので使う
            pass
        else:
            # HTTPで落とす
            logger.info("Crawl URL", url=url)

            content = await self.fetch_content(url, check_status)
            if not content:
                raise CrawlerError("Fetch failed")

            if content.status_code == 200:
                await self.content_store.push_content(self, url, content)

        return parser(url, params, content)

    async def crawl_image(self, url: Union[URL, str], check_status: bool = True) -> BinaryData:
        rv = await self.crawl(url, parser=parse_image, check_status=check_status)
        return cast(BinaryData, rv)

    async def dispatch(self, planner: str, args: Mapping[str, Any], **options: Dict[str, Any]) -> None:
        logger.info("Dispatch", planner=planner, args=args, options=options)
        await self.scheduler.dispatch(self, planner, args, options)

    async def get_context(self, context_name: str) -> "SessionContext":
        context = SessionContext(self, self.context_store, context_name)
        await context.load()
        return context

    # internal
    async def close(self) -> None:
        await self.aio_session.close()
        await self.context_store.close()

    # private
    async def handle_planner(self, planner: str, args: Mapping[str, Any]) -> Any:
        logger.info("Handle planner", planner=planner, args=args)

        # TODO: check exists
        p = self.blackcat.planners[planner]
        return await p(self, **args)

    async def fetch_content(self, url: URL, check_status: bool) -> Optional[FetchedContent]:
        # ホストにアクセスする間隔についてwaitを入れる
        if url.scheme not in ("http", "https") or not url.host or not url.port:
            return None

        now = time.time()
        host_key = (url.host, url.port)
        last_request = self.last_request_time_per_host.get(host_key, None)
        if last_request:
            wait_until = last_request + self.blackcat.request_interval
            if wait_until > now:
                logger.debug("sleep %f secs", wait_until - now)
                await asyncio.sleep(wait_until - now)
        self.last_request_time_per_host[host_key] = now

        # crawl and parse
        async with self.aio_session.get(url) as resp:
            if check_status:
                if resp.status != 200:
                    raise BadStatusCode(resp.status)

            return FetchedContent(resp.status, resp.headers, await resp.read())


# SessionContext
SessionAttrValueT = TypeVar("SessionAttrValueT")


class SessionContext:
    session: BlackcatSession
    store: ContextStore
    context_name: str
    _data: Mapping[str, Any]

    def __init__(self, session: BlackcatSession, store: ContextStore, context_name: str):
        self.session = session
        self.store = store
        self.context_name = context_name

    # public
    async def get(self, attr: str, default: Optional[SessionAttrValueT] = None) -> SessionAttrValueT:
        rv = self._data.get(attr, default)
        return cast(SessionAttrValueT, rv)

    async def set(self, attr: str, value: SessionAttrValueT) -> None:
        self._data[attr] = value  # type: ignore
        await self.save()

    # internal
    async def load(self) -> None:
        try:
            data = await self.store.load(self.session, self.context_name)
            logger.info("Load context", context=self.context_name, data=repr(data))
        except ContextNotFoundError:
            data = {}
            logger.info("Context not found", context=self.context_name)
        self._data = data

    async def save(self) -> None:
        logger.info("Save context", context=self.context_name, data=repr(self._data))
        await self.store.save(self.session, self.context_name, self._data)


# ContentParser
def parse_image(url: URL, params: Dict[str, str], content: Content) -> BinaryData:
    return BinaryData(url, content.content_type, content.body)
