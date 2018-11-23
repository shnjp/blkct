# -*- coding:utf-8 -*-
import importlib
import os

import click

# from blkct.environment import OTCrawlerEnvironment
from .logging import init_logging, logger

# _PREFIX = 'BLKCT_'
# ENV_CONFIG = '{}_CONFIG'.format(_PREFIX)
# ENV_DEBUG = '{}_DEBUG'.format(_PREFIX)
# DEFAULT_CONFIG_FILE_NAME = 'otcrawler.ini'


@click.group()
# @click.option('--config', '-c', type=click.Path(exists=True), envvar=ENV_CONFIG, default=DEFAULT_CONFIG_FILE_NAME)
@click.option('--debug', is_flag=True)
@click.pass_context
def cli_main(ctx: click.Context, debug: bool) -> None:
    """otcrawler management commands

    :param Context ctx: Context
    """

    init_logging(debug=debug)
    logger.info('Start blkct; debug=%r', debug)
    # env = OTCrawlerEnvironment(config, debug=debug)
    # ctx.obj = env


def _import_subcommands() -> None:
    cli_dir = os.path.join(os.path.split(__file__)[0], 'cli')
    for fn in os.listdir(cli_dir):
        file_path = os.path.join(cli_dir, fn)
        if os.path.isdir(file_path) and os.path.exists(os.path.join(file_path, '__init__.py')):
            module_name = fn
        elif os.path.isfile(file_path) and fn.endswith('.py') and not fn.startswith('__'):
            module_name = fn.split('.', 1)[0]
        else:
            continue

        try:
            mod = importlib.import_module('blkct.cli.{}'.format(module_name), __name__)
        except ImportError:
            logger.exception(f'failed to import module `{module_name}`')
            continue

        try:
            group = getattr(mod, 'cli_{}'.format(module_name))
        except AttributeError:
            pass
        else:
            cli_main.add_command(group)


_import_subcommands()
del _import_subcommands
