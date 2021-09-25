import os
from dataclasses import dataclass, replace
from typing import Optional


@dataclass
class ConnectionData:
    hostname: str
    username: str
    password: Optional[str] = None
    keyfile: Optional[str] = None
    key: Optional[str] = None
    port: int = 22

    @staticmethod
    def with_resolved_keyfile(connection_data: 'ConnectionData') -> 'ConnectionData':
        return ConnectionData._resolve_keyfile_in_connection(connection_data)

    @staticmethod
    def _resolve_keyfile_in_connection(connection):
        keyfile = ConnectionData._resolve_keyfile_from_home_dir(connection.keyfile)
        connection = replace(connection, keyfile=keyfile)
        return connection

    @staticmethod
    def _resolve_keyfile_from_home_dir(keyfile: str) -> Optional[str]:
        home_dir = os.environ["HOME"]
        if not keyfile:
            return None

        if keyfile.startswith("~/"):
            keyfile = keyfile.replace("~/", home_dir + "/", 1)

        return keyfile
