from __future__ import annotations

import re
from typing import NamedTuple, TYPE_CHECKING, TypeVar, cast

if TYPE_CHECKING:
    from typing import Callable, List, Dict, Optional, Pattern, Tuple, Union

    from .typing import ContentParserType, PlannerType

RT = TypeVar("RT")


class ContentParserEntry(NamedTuple):
    pattern: Pattern[str]
    parse: ContentParserType


class BlackcatSetup:
    content_parsers: List[ContentParserEntry]
    planners: Dict[str, PlannerType]

    def __init__(self) -> None:
        self.planners = {}
        self.content_parsers = []

    def register_planner(
        self, name: Optional[str] = None
    ) -> Callable[[PlannerType], PlannerType]:
        def decorator(f: PlannerType) -> PlannerType:
            self.register_planner_func(f, name)
            return f

        return decorator

    def register_content_parser(
        self,
        url_pattern: Union[str, Pattern[str]],
        flags: re.RegexFlag = cast(re.RegexFlag, 0),
    ) -> Callable[[ContentParserType], ContentParserType]:
        def decorator(f: ContentParserType) -> ContentParserType:
            self.register_content_parser_func(url_pattern, flags, f)
            return f

        return decorator

    def register_planner_func(self, f: PlannerType, name: Optional[str] = None) -> None:
        if not name:
            name = f.__name__
        if name in self.planners:
            raise ValueError(f"planner `{name}` is already registered.")
        self.planners[name] = f

    def register_content_parser_func(
        self,
        url_pattern: Union[str, Pattern[str]],
        re_flags: re.RegexFlag,
        f: ContentParserType,
    ) -> None:
        pattern = re.compile(url_pattern, re_flags)
        self.content_parsers.append(ContentParserEntry(pattern, f))


def merge_setups(
    setups: List[BlackcatSetup]
) -> Tuple[Dict[str, PlannerType], List[ContentParserEntry]]:
    planners: Dict[str, PlannerType] = {}
    content_parsers: List[ContentParserEntry] = []

    for setup in setups:
        conflicted_planners = set(setup.planners.keys()) & set(planners.keys())
        if conflicted_planners:
            raise ValueError(f'planner {", ".join(conflicted_planners)} conflicted')

        planners.update(setup.planners)
        content_parsers.extend(setup.content_parsers)

    return planners, content_parsers
