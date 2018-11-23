# -*- coding:utf-8 -*-
import asyncio
import importlib

import click

from blkct.logging import logger
from blkct.spider import Spider


@click.command('crawl', context_settings=dict(allow_extra_args=True, ignore_unknown_options=True))
@click.argument('plan')
@click.pass_context
def cli_crawl(ctx, plan):
    """planを指定してクロールする"""
    # parse args for plan
    plan_args = {}
    for arg in ctx.args:
        if not arg.startswith('--') or '=' not in arg:
            raise click.BadParameter('Bad plan option {}'.format(arg))
        opt, value = arg.split('=', 1)
        assert opt.startswith('--')
        opt = opt[2:]
        plan_args[opt] = value

    module_name, plan_name = plan.rsplit(':')

    try:
        mod = importlib.import_module(module_name)
    except ImportError:
        logger.exception(f'failed to import module {module_name}')
        raise click.Abort()

    plan = getattr(mod, 'run_{}'.format(plan_name))

    spider = Spider()

    ioloop = asyncio.get_event_loop()
    ioloop.run_until_complete(spider.run(plan, **plan_args))
