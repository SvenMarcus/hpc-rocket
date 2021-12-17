import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class ConnectionData:
    hostname: str
    username: str
    password: Optional[str] = None
    keyfile: Optional[str] = None
    key: Optional[str] = None
    port: int = 22

    def __post_init__(self) -> None:
        self._resolve_keyfile()

    def _resolve_keyfile(self) -> None:
        self.keyfile = ConnectionData._resolve_keyfile_from_home_dir(self.keyfile)

    @staticmethod
    def _resolve_keyfile_from_home_dir(keyfile: Optional[str]) -> Optional[str]:
        home_dir = os.environ["HOME"]
        if not keyfile:
            return None

        if keyfile.startswith("~/"):
            keyfile = keyfile.replace("~/", home_dir + "/", 1)

        return keyfile
