# -*- coding:utf-8 -*-
import collections
import logging
import sys
from typing import Any, Iterator, Mapping, MutableMapping, Tuple, cast

from typing_extensions import Protocol

try:
    import colorama

    has_colorama = True
except ImportError:
    has_colorama = False

__all__ = ("init_logging", "logger")


class BlackcatLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg: Any, kwargs: MutableMapping[str, Any]) -> Tuple[Any, MutableMapping[str, Any]]:
        exc_info = kwargs.pop("exc_info", None)
        extra = kwargs.pop("extra", None)
        stack_info = kwargs.pop("stack_info", False)
        _structual = kwargs

        if not isinstance(extra, dict):
            extra = dict()
        extra.update(self.extra)
        extra["_kwargs"] = kwargs

        return msg, dict(exc_info=exc_info, extra=extra, stack_info=stack_info)


class BlackcatLTSVFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        # dictオブジェクト並び順が保証されない？のでDjango SortedDictを使っている
        data: MutableMapping[str, Any] = collections.OrderedDict()
        data["level"] = record.levelname
        data["name"] = record.name
        data["time"] = record.created
        if hasattr(record, "session_id"):
            data["session_id"] = cast(Any, record).session_id

        if record.exc_info:
            exc_text = self.formatException(record.exc_info)
            data["exc_info"] = exc_text

        # ログメッセージが辞書の場合には出力データにそのままマッピングする
        if hasattr(record, "_kwargs"):
            data["message"] = record.msg
            data.update(cast(Any, record)._kwargs)
        else:
            data["message"] = record.msg
            data["args"] = record.args

        data = self._post_process(record, data)

        # LTSV
        def _iter(d: Mapping[str, Any]) -> Iterator[str]:
            for k, v in d.items():
                if not isinstance(v, str):
                    v = str(v)
                v = v.replace("\n", r"\n").replace("\t", r"\t")

                yield f"{k}:{v}"

        return "\t".join(_iter(data))

    def _post_process(self, record: logging.LogRecord, data: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
        return data


if has_colorama:

    def _colored(color: str, text: str) -> str:
        return f"{color}{text}{colorama.Style.RESET_ALL}"

    class BlackcatColoredLTSVFormatter(BlackcatLTSVFormatter):
        level_color_map = {
            "DEBUG": colorama.Style.DIM + colorama.Fore.WHITE,
            "INFO": colorama.Fore.WHITE,
            "WARNING": colorama.Fore.YELLOW,
            "ERROR": colorama.Fore.RED,
            "CRITICAL": colorama.Style.BRIGHT + colorama.Fore.RED + colorama.Back.WHITE,
        }

        def _post_process(self, record: logging.LogRecord, data: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
            data["level"] = _colored(self.level_color_map[record.levelname], record.levelname)
            data["name"] = _colored(colorama.Fore.MAGENTA, data["name"])
            return data


def init_logging(verbose: bool) -> None:
    # init logging
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)

    fh = logging.StreamHandler(sys.stderr)

    # if we are attached to tty, use colorful.
    if sys.stderr.isatty() and has_colorama:
        fh.setFormatter(BlackcatColoredLTSVFormatter())
    else:
        fh.setFormatter(BlackcatLTSVFormatter())
    logger.logger.addHandler(fh)
    logger.logger.propagate = False
    logger.logger.setLevel(logging.DEBUG if verbose else logging.INFO)


def set_session_id_to_log(session_id: str) -> None:
    """blckt loggerにsession_idパラメタを追加する"""
    logger.extra["session_id"] = session_id  # type: ignore


logger = BlackcatLoggerAdapter(logging.getLogger("blckt"), {})
