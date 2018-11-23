# -*- coding:utf-8 -*-
import asyncio
import importlib
import json
import re

import click

from .blackcat import Blackcat
from .logging import init_logging, logger


def easy_json_loads(s):
    """json.loadsだけど、'{}'がついてなければ追加する"""
    if not s.startswith('{'):
        s = f'{{{s}}}'
    return json.loads(s)


@click.command(context_settings=dict(ignore_unknown_options=True))
@click.option('--workers', default=4, type=int)
def _make_asyncio_scheduler(workers, **kwargs):
    """
    内部用コマンド
    asyncioを使ったスケジューラを作成する
    """
    from .scheduler.asyncio_scheduler import AsyncIOScheduler
    return AsyncIOScheduler(num_workers=workers)


@click.command(context_settings=dict(auto_envvar_prefix='BLKCT', ignore_unknown_options=True))
@click.option('--scheduler', default='asyncio')
@click.option('-m', '--module', 'modules', multiple=True)
@click.option('-v', '--verbose', is_flag=True)
@click.option('--user-agent')
@click.argument('planner')
@click.argument('argument', default=None, type=easy_json_loads, required=False)
@click.pass_context
def blackcat(ctx, planner, argument=None, scheduler='asyncio', modules=[], verbose=False, user_agent=None):
    """
    blackcat
    """
    # TODO: schedulerサブコマンドのhelpをprint出来るように
    init_logging(verbose=verbose)

    # schedulerを読み込む。
    if not re.match(r'\w+', scheduler):
        raise click.BadParameter('bad scheduler name')

    # make blackcat
    blackcat = Blackcat(
        scheduler_factory=lambda: ctx.forward(globals()[f'_make_{scheduler}_scheduler']), user_agent=user_agent
    )

    # load planners/parsers
    with blackcat.setup():
        for module in modules:
            logger.info(f'Load module {module}')
            importlib.import_module(module)

    # run
    loop = asyncio.get_event_loop()
    loop.run_until_complete(blackcat.run_with_session(planner, **({} if argument is None else argument)))


if __name__ == '__main__':
    blackcat()
