from typing import Any, Dict, Union

from envyaml import EnvYAML

from hpcrocket.core.filesystem import Filesystem


class ParseError(RuntimeError):
    def __init__(self, reason: str) -> None:
        super().__init__()
        self._reason = reason

    def __str__(self) -> str:
        return self._reason


def parse_yaml(path: str, filesystem: Filesystem) -> Union[Dict[str, Any], ParseError]:
    try:
        return EnvYAML(path)
    except FileNotFoundError:
        return ParseError(f"File {path} does not exist!")
