from __future__ import annotations

from typing import TYPE_CHECKING, cast

from werkzeug.local import LocalProxy, LocalStack

if TYPE_CHECKING:
    from .blackcat import Blackcat


def _find_setup() -> Blackcat:
    top = _setup_ctx_stack.top
    if top is None:
        raise RuntimeError('Working outside of blackcat setup context.')

    if TYPE_CHECKING:
        return cast(Blackcat, top.blackcat)
    else:
        return top.blackcat


_setup_ctx_stack = LocalStack()  # type: ignore
current_blackcat = LocalProxy(_find_setup)  # type: ignore
