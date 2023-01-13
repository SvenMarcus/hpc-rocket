import os
from typing import Callable, Tuple


def removeprefix(string: str, prefix: str) -> str:
    def __removeprefix(prefix: str) -> str:
        if string.startswith(prefix):
            len_prefix = len(prefix)
            return string[len_prefix:]

        return string

    _removeprefix: Callable[[str], str] = getattr(
        string, "removeprefix", __removeprefix
    )
    return _removeprefix(prefix)


def is_glob(path: str) -> bool:
    """
    Checks if a wildcard operator is used in the path

    Args:
        path (str): The filepath

    Returns:
        bool
    """

    return "*" in path


def path_after_wildcard(glob_pattern: str, full_filepath: str) -> str:
    dir, _ = split_at_first_wildcard(glob_pattern)
    filename = removeprefix(full_filepath, dir)
    return removeprefix(filename, os.path.sep)


def split_at_first_wildcard(pattern: str) -> Tuple[str, str]:
    first_wildcard = _first_wildcard(pattern)
    return pattern[:first_wildcard], pattern[first_wildcard:]


def _first_wildcard(pattern: str) -> int:
    first_star = pattern.find("*")
    if first_star == -1:
        first_star = 0

    return first_star
