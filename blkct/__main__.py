from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import sys
from typing import TYPE_CHECKING, cast

from .blackcat import Blackcat
from .logging import init_logging, logger
from .utils import make_new_session_id

if TYPE_CHECKING:
    from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

    from .typing import ContentStoreFactory, ContentStore, Scheduler, SchedulerFactory

SCHEDULER_FACTORIES: Dict[str, Tuple[Callable[[], argparse.ArgumentParser], Callable[[argparse.
                                                                                      Namespace], Scheduler]]] = {}
CONTENT_STORE_FACTORIES: Dict[str, Tuple[Callable[[], argparse.
                                                  ArgumentParser], Callable[[argparse.Namespace], ContentStore]]] = {}


def easy_json_loads(s: str) -> Mapping[str, Any]:
    """json.loadsだけど、'{}'がついてなければ追加する"""
    print('!?', s)
    if not s.startswith('{'):
        s = f'{{{s}}}'
    rv = json.loads(s)
    for key in rv:
        if not isinstance(key, str):
            raise ValueError(f'key {key} is not str')
    return cast(Mapping[str, Any], rv)


# AsyncIOScheduler
def _make_asyncio_scheduler_argparser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser()


def _make_asyncio_scheduler(args: argparse.Namespace) -> Scheduler:
    """
    内部用コマンド
    asyncioを使ったスケジューラを作成する
    """
    from .scheduler.asyncio_scheduler import AsyncIOScheduler

    logger.info('make AsyncIOScheduler')

    return AsyncIOScheduler()


SCHEDULER_FACTORIES['asyncio'] = _make_asyncio_scheduler_argparser, _make_asyncio_scheduler


# FileContentStore
def _make_file_content_store_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--content-store-path')

    return parser


def _make_file_content_store(args: argparse.Namespace) -> ContentStore:
    """
    内部用コマンド
    asyncioを使ったスケジューラを作成する
    """
    from .content_store.file_content_store import FileContentStore

    logger.info('make FileContentStore at %s', args.content_store_path)

    return FileContentStore(store_root_path=args.content_store_path)


CONTENT_STORE_FACTORIES['file'] = _make_file_content_store_argparser, _make_file_content_store


# main
def blackcat(
    scheduler_factory: SchedulerFactory, content_store_factory: ContentStoreFactory, planner: str,
    argument: Dict[str, Any], modules: List[str], session_id: Optional[str], verbose: bool, user_agent: Optional[str]
) -> None:
    """
    blackcat
    """
    # TODO: schedulerサブコマンドのhelpをprint出来るように
    init_logging(verbose=verbose)

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


def parse_args(args: List[str]) -> Tuple[argparse.Namespace, argparse.Namespace, argparse.Namespace]:
    main_parser = argparse.ArgumentParser()
    main_parser.add_argument('--scheduler', default='asyncio')
    main_parser.add_argument('--content-store', default='file')
    main_parser.add_argument('-m', '--module', nargs='*', dest='modules')
    main_parser.add_argument('--session-id')
    main_parser.add_argument('-v', action='store_true', dest='verbose')
    main_parser.add_argument('--user-agent')
    main_parser.add_argument('planner', metavar='PLANNER')
    main_parser.add_argument('argument', nargs='?', metavar='ARGUMENT')

    main_args, args = main_parser.parse_known_args(args)
    scheduler_args, args = SCHEDULER_FACTORIES[main_args.scheduler][0]().parse_known_args(args)
    content_store_args, args = CONTENT_STORE_FACTORIES[main_args.content_store][0]().parse_known_args(args)

    if args:
        print('Unknown options/args', file=sys.stderr)
        for arg in args:
            print(f'  {arg}', file=sys.stderr)
        sys.exit(1)

    return main_args, scheduler_args, content_store_args


def main(args: Optional[List[str]] = None) -> None:
    if not args:
        args = sys.argv[1:]
    main_args, scheduler_args, content_store_args = parse_args(args)

    # make blackcat
    blackcat(
        lambda: SCHEDULER_FACTORIES[main_args.scheduler][1](scheduler_args),
        lambda: CONTENT_STORE_FACTORIES[main_args.content_store][1](content_store_args), main_args.planner,
        main_args.argument, main_args.modules, main_args.session_id, main_args.verbose, main_args.user_agent
    )


if __name__ == '__main__':
    main()
