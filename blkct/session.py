import asyncio
import time

from yarl import URL

from .exceptions import BadStatusCode
from .logging import logger


class BlackcatSession:

    def __init__(self, blackcat, scheduler):
        self.blackcat = blackcat
        self.scheduler = scheduler
        self.aio_session = self.blackcat.make_aio_session()
        self.last_request_time_per_host = {}

    # public
    async def crawl(self, url, *, parser=None, check_status=True):
        # check if parser exists
        if not parser:
            parser, params = self.blackcat.get_content_parsers_by_url(url)
            if not parser:
                raise Exception(f'No parsers found for URL `{url}`')
        else:
            params = None

        url = URL(url) if isinstance(url, str) else url
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

    async def dispatch(self, planner: str, **args) -> None:
        logger.info('Dispatch %s with args %r', planner, args)
        await self.scheduler.dispatch(self, planner, args)

    # internal
    async def close(self):
        await self.aio_session.close()

    # private
    async def handle_planner(self, planner, args):
        logger.info('Handle planner %s with args %r', planner, args)

        # TODO: check exists
        p = self.blackcat.planners[planner]
        return await p(self, **args)

    async def handle_crawler(self):
        pass
