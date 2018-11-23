from werkzeug.local import LocalProxy, LocalStack


def _find_setup():
    top = _setup_ctx_stack.top
    if top is None:
        raise RuntimeError('Working outside of blackcat setup context.')
    return top.blackcat


_setup_ctx_stack = LocalStack()
current_blackcat = LocalProxy(_find_setup)
