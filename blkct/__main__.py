from __future__ import annotations

import argparse
import asyncio
import importlib
import itertools
import json
import logging
import os
import sys
from gettext import gettext as _
from typing import TYPE_CHECKING, TypeVar, cast

from .blackcat import Blackcat
from .logging import init_logging, logger, set_session_id_to_log
from .setup import merge_setups
from .utils import make_new_session_id

if TYPE_CHECKING:
    from typing import Any, Callable, Dict, List, Optional, Tuple, Union, Sequence

    from .typing import (
        ContentStoreFactory,
        ContentStore,
        ContextStore,
        ContextStoreFactory,
        Scheduler,
        SchedulerFactory,
    )

DefaultT = TypeVar("DefaultT")  # default_or_environ()用
SCHEDULER_FACTORIES: Dict[
    str, Tuple[Callable[[], argparse.ArgumentParser], Callable[[argparse.Namespace], Scheduler]]
] = {}
CONTENT_STORE_FACTORIES: Dict[
    str, Tuple[Callable[[], argparse.ArgumentParser], Callable[[argparse.Namespace], ContentStore]]
] = {}
CONTEXT_STORE_FACTORIES: Dict[
    str, Tuple[Callable[[], argparse.ArgumentParser], Callable[[argparse.Namespace], ContextStore]]
] = {}


# AsyncIOScheduler
def _make_asyncio_scheduler_argparser() -> argparse.ArgumentParser:
    return make_argument_parser(prog="AsyncIO Scheduler")


def _make_asyncio_scheduler(args: argparse.Namespace) -> Scheduler:
    """
    内部用コマンド
    asyncioを使ったスケジューラを作成する
    """
    from .scheduler.asyncio_scheduler import AsyncIOScheduler

    logger.info("make AsyncIOScheduler")

    return AsyncIOScheduler()


SCHEDULER_FACTORIES["asyncio"] = (_make_asyncio_scheduler_argparser, _make_asyncio_scheduler)


# AWSBatchScheduler
def _make_awsbatch_scheduler_argparser() -> argparse.ArgumentParser:
    parser = make_argument_parser(prog="AWSBatch Scheduler")
    parser.add_argument("--job-definition", default=default_or_environ("BLKCT_AWSBATCH_JOB_DEFINITION"))
    parser.add_argument("--job-queue", default=default_or_environ("BLKCT_AWSBATCH_JOB_QUEUE"))
    return parser


def _make_awsbatch_scheduler(args: argparse.Namespace) -> Scheduler:
    """
    awsbatchを使ったスケジューラを作成する
    """
    from .scheduler.awsbatch_scheduler import AWSBatchScheduler

    logger.info("make AWSBatchScheduler", job_definition=args.job_definition, job_queue=args.job_queue)

    return AWSBatchScheduler(args.job_definition, args.job_queue)


SCHEDULER_FACTORIES["awsbatch"] = (_make_awsbatch_scheduler_argparser, _make_awsbatch_scheduler)


# FileContentStore
def _make_file_content_store_argparser() -> argparse.ArgumentParser:
    parser = make_argument_parser(prog="File Content Store")
    parser.add_argument("--content-store-path", default=default_or_environ("BLKCT_CONTENT_STORE_PATH", "/tmp/blkct"))

    return parser


def _make_file_content_store(args: argparse.Namespace) -> ContentStore:
    """
    内部用コマンド
    ローカルストレージに保存するContentStoreを作る
    """
    from .content_store.file_content_store import FileContentStore

    logger.info("make FileContentStore", path=args.content_store_path)

    return FileContentStore(store_root_path=args.content_store_path)


CONTENT_STORE_FACTORIES["file"] = (_make_file_content_store_argparser, _make_file_content_store)


# S3ContentStore
def _make_s3_content_store_argparser() -> argparse.ArgumentParser:
    parser = make_argument_parser(prog="S3 Content Store")
    parser.add_argument("--s3-content-bucket", default=default_or_environ("BLKCT_S3_CONTENT_BUCKET"))
    parser.add_argument("--s3-content-prefix", default=default_or_environ("BLKCT_S3_CONTENT_PREFIX"))

    return parser


def _make_s3_content_store(args: argparse.Namespace) -> ContentStore:
    """
    S3に保存するContentStoreを作る
    """
    from .content_store.s3_content_store import S3ContentStore

    logger.info("make S3ContentStore", bucket=args.s3_content_bucket, content_prefix=args.s3_content_prefix)

    return S3ContentStore(args.s3_content_bucket, args.s3_content_prefix)


CONTENT_STORE_FACTORIES["s3"] = _make_s3_content_store_argparser, _make_s3_content_store


# FileContextStore
def _make_file_context_store_argparser() -> argparse.ArgumentParser:
    parser = make_argument_parser(prog="File Context Store")
    parser.add_argument("--db-file-path", default=default_or_environ("BLKCT_DB_FILE_PATH"))

    return parser


def _make_file_context_store(args: argparse.Namespace) -> ContextStore:
    """
    DBファイルにContextを保存する
    """
    from .context_store.file_context_store import FileContextStore

    assert args.db_file_path

    logger.info("make FileContextStore")

    return FileContextStore(args.db_file_path)


CONTEXT_STORE_FACTORIES["file"] = (_make_file_context_store_argparser, _make_file_context_store)


# DynamDBContextStore
def _make_dynamodb_context_store_argparser() -> argparse.ArgumentParser:
    parser = make_argument_parser(prog="DynamoDB Context Store")
    parser.add_argument("--dynamodb-table-name", default=default_or_environ("BLKCT_DYNAMODB_TABLE_NAME"))

    return parser


def _make_dynamodb_context_store(args: argparse.Namespace) -> ContextStore:
    """
    DBファイルにContextを保存する
    """
    from .context_store.dynamodb_context_store import DynamoDBContextStore

    logger.info("make DynamoDBContextStore")

    return DynamoDBContextStore(args.dynamodb_table_name)


CONTEXT_STORE_FACTORIES["dynamodb"] = (_make_dynamodb_context_store_argparser, _make_dynamodb_context_store)


# main
class BlackcatHelpAction(argparse.Action):
    """blkct本体と、scheduler, content storeの全Optionを出力するためのHelp Action"""

    def __init__(
        self,
        option_strings: str,
        dest: str = argparse.SUPPRESS,
        default: Any = argparse.SUPPRESS,
        help: Optional[str] = None,
    ):
        super(BlackcatHelpAction, self).__init__(
            option_strings=option_strings, dest=dest, default=default, nargs=0, help=help
        )

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Union[str, Sequence[Any], None],
        option_string: Optional[str] = None,
    ) -> None:
        parser.print_help()

        formatter = parser._get_formatter()

        # show submodule helps
        for x, parser_factory in itertools.chain(
            sorted((k, v[0]) for k, v in SCHEDULER_FACTORIES.items()),
            sorted((k, v[0]) for k, v in CONTENT_STORE_FACTORIES.items()),
        ):
            subparser = parser_factory()
            formatter.start_section(f"{subparser.prog} options")
            formatter.add_text(subparser.description)
            formatter.add_arguments(subparser._optionals._group_actions)
            formatter.end_section()

        print("\n")
        print(formatter.format_help())

        parser.exit()


def make_argument_parser(**kwargs: Any) -> argparse.ArgumentParser:
    main_parser = argparse.ArgumentParser(add_help=False, **kwargs)
    return main_parser


def default_or_environ(env: str, default: Optional[DefaultT] = None) -> DefaultT:
    return cast(DefaultT, os.environ.get(env, default))


# yapf: disable
def parse_args(args: List[str]
               ) -> Tuple[argparse.Namespace, argparse.Namespace, argparse.Namespace, argparse.Namespace]:
    # yapf: enable
    main_parser = make_argument_parser()
    main_parser.add_argument('--scheduler', default=default_or_environ('BLKCT_SCHEDULER', 'asyncio'))
    main_parser.add_argument('--content-store', default=default_or_environ('BLKCT_CONTENT_STORE', 'file'))
    main_parser.add_argument('--context-store', default=default_or_environ('BLKCT_CONTEXT_STORE', 'file'))
    main_parser.add_argument(
        '-m', '--module', nargs='*', dest='modules', default=default_or_environ('BLKCT_MODULES', [])
    )
    main_parser.add_argument('--session-id', default=default_or_environ('BLKCT_SESSION_ID'))
    main_parser.add_argument(
        '-v', action='store_true', dest='verbose', required=False, default=default_or_environ('BLKCT_VERBOSE', False)
    )
    main_parser.add_argument('--user-agent', default=default_or_environ('BLKCT_USER_AGENT'))
    main_parser.add_argument('planner', metavar='PLANNER')
    main_parser.add_argument('argument', nargs='?', metavar='ARGUMENT')
    main_parser.add_argument('-h', '--help', action=BlackcatHelpAction, help=_('show this help message and exit'))

    main_args, args = main_parser.parse_known_args(args)
    scheduler_args, args = SCHEDULER_FACTORIES[main_args.scheduler][0]().parse_known_args(args)
    content_store_args, args = CONTENT_STORE_FACTORIES[main_args.content_store][0]().parse_known_args(args)
    context_store_args, args = CONTEXT_STORE_FACTORIES[main_args.context_store][0]().parse_known_args(args)

    if args:
        print('Unknown options/args', file=sys.stderr)
        for arg in args:
            print(f'  {arg}', file=sys.stderr)
        sys.exit(1)

    return main_args, scheduler_args, content_store_args, context_store_args


def blackcat(
    scheduler_factory: SchedulerFactory, content_store_factory: ContentStoreFactory,
    context_store_factory: ContextStoreFactory, planner: str, argument: Dict[str, Any], modules: List[str],
    session_id: Optional[str], verbose: bool, user_agent: Optional[str]
) -> None:
    """
    blackcat
    """
    init_logging(verbose=verbose)
    logging.getLogger('botocore').setLevel(logging.WARN)

    # load planners/parsers
    setups = []
    for path in modules:
        t = path.split(':', 1)
        if len(t) == 2:
            module_name, setup_name = t
        else:
            module_name, setup_name = t[0], 'blackcat_setup'

        logger.info("Load module", module=f"{module_name}:{setup_name}")
        module = importlib.import_module(module_name)
        setup = getattr(module, setup_name)
        setups.append(setup)

    planners, content_parsers = merge_setups(setups)
    blackcat = Blackcat(
        planners=planners,
        content_parsers=content_parsers,
        scheduler_factory=scheduler_factory,
        content_store_factory=content_store_factory,
        context_store_factory=context_store_factory,
        user_agent=user_agent,
    )

    # make session id
    session_id = session_id or make_new_session_id()
    set_session_id_to_log(session_id)

    # run
    loop = asyncio.get_event_loop()
    loop.run_until_complete(blackcat.run_with_session(planner, {} if argument is None else argument, session_id))


def main(args: Optional[List[str]] = None) -> None:
    if not args:
        args = sys.argv[1:]
    main_args, scheduler_args, content_store_args, context_store_args = parse_args(args)

    # modulesが環境変数から来た場合、リストでなく、文字列なので治す
    modules = main_args.modules
    if isinstance(modules, str):
        modules = modules.split(',')

    argument = main_args.argument
    if argument:
        argument = json.loads(argument)

    # make blackcat
    blackcat(
        lambda: SCHEDULER_FACTORIES[main_args.scheduler][1](scheduler_args),
        lambda: CONTENT_STORE_FACTORIES[main_args.content_store][1](content_store_args),
        lambda: CONTEXT_STORE_FACTORIES[main_args.context_store][1](context_store_args), main_args.planner, argument,
        modules, main_args.session_id, main_args.verbose, main_args.user_agent
    )


if __name__ == '__main__':
    main()
