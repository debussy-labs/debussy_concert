"""Small callables motifs use as task actions."""


def identity(value):
    """Return ``value``: makes a rendered template a task result that later
    tasks can read with ``result_of``."""
    return value
