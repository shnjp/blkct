from __future__ import annotations

import asyncio
import importlib
import json
import re
from typing import TYPE_CHECKING, cast

import click

from .blackcat import Blackcat
from .logging import init_logging, logger
from .utils import make_new_session_id

if TYPE_CHECKING:
    from typing import Any, Dict, List, Mapping, Optional

    from .typing import ContentStore, Scheduler


def easy_json_loads(s: str) -> Mapping[str, Any]:
    """json.loadsだけど、'{}'がついてなければ追加する"""
    if not s.startswith('{'):
        s = f'{{{s}}}'
    rv = json.loads(s)
    for key in rv:
        if not isinstance(key, str):
            raise click.BadParameter(f'key {key} is not str')
    return cast(Mapping[str, Any], rv)


@click.command(context_settings=dict(ignore_unknown_options=True))
@click.option('--workers', default=4, type=int)
def _make_asyncio_scheduler(workers: int, **kwargs: Dict[str, Any]) -> Scheduler:
    """
    内部用コマンド
    asyncioを使ったスケジューラを作成する
    """
    from .scheduler.asyncio_scheduler import AsyncIOScheduler

    logger.info('make AsyncIOScheduler num_workers=%d', workers)

    return AsyncIOScheduler(num_workers=workers)


@click.command(context_settings=dict(ignore_unknown_options=True))
@click.option('--store-path', type=click.Path())
def _make_file_content_store(store_path: str, **kwargs: Dict[str, Any]) -> ContentStore:
    """
    内部用コマンド
    asyncioを使ったスケジューラを作成する
    """
    from .content_store.file_content_store import FileContentStore

    logger.info('make FileContentStore store_root_path=%s', store_path)

    return FileContentStore(store_root_path=store_path)


@click.command(context_settings=dict(auto_envvar_prefix='BLKCT', ignore_unknown_options=True))
@click.option('--scheduler', default='asyncio')
@click.option('--store', 'content_store', default='file')
@click.option('-m', '--module', 'modules', multiple=True)
@click.option('-v', '--verbose', is_flag=True)
@click.option('--session-id')
@click.option('--user-agent')
@click.argument('planner')
@click.argument('argument', default=None, type=easy_json_loads, required=False)
@click.pass_context
def blackcat(
    ctx: click.Context,
    scheduler: str,
    content_store: str,
    planner: str,
    modules: List[str] = [],
    argument: Optional[Dict[str, Any]] = None,
    session_id: Optional[str] = None,
    verbose: bool = False,
    user_agent: Optional[str] = None
) -> None:
    """
    blackcat
    """
    # TODO: schedulerサブコマンドのhelpをprint出来るように
    init_logging(verbose=verbose)

    # schedulerを読み込む。
    if not re.match(r'\w+', scheduler):
        raise click.BadParameter('bad scheduler name')

    # make blackcat
    def scheduler_factory() -> Scheduler:
        rv = ctx.forward(globals()[f'_make_{scheduler}_scheduler'])
        if TYPE_CHECKING:
            return cast(Scheduler, rv)
        return rv

    def content_store_factory() -> ContentStore:
        rv = ctx.forward(globals()[f'_make_{content_store}_content_store'])
        if TYPE_CHECKING:
            return cast(ContentStore, rv)
        return rv

    blackcat = Blackcat(
        scheduler_factory=scheduler_factory, content_store_factory=content_store_factory, user_agent=user_agent
    )

    # load planners/parsers
    with blackcat.setup():
        for module in modules:
            logger.info(f'Load module {module}')
            importlib.import_module(module)

    # make session id
    session_id = session_id or make_new_session_id()

    # run
    loop = asyncio.get_event_loop()
    loop.run_until_complete(blackcat.run_with_session(planner, {} if argument is None else argument, session_id))


if __name__ == '__main__':
    blackcat()
