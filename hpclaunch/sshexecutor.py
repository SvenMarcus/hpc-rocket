from typing import Optional

from hpclaunch.errors import SSHError
from hpclaunch.executor import CommandExecutor, RunningCommand
from hpclaunch.sshclient import ConnectionData, SSHExecutor as SSHClient


class SSHExecutor(CommandExecutor):
    def __init__(self, hostname: str) -> None:
        self._hostname: str = hostname
        self._client = SSHClient()

    @property
    def is_connected(self) -> bool:
        return self._client.is_connected

    def load_host_keys_from_file(self, hostfile: str) -> None:
        self._client.load_host_keys_from_file(hostfile)

    def connect(
            self, username: str, keyfile: Optional[str] = None, password: Optional[str] = None,
            private_key: Optional[str] = None) -> None:

        connection = ConnectionData(hostname=self._hostname,
                                    password=password,
                                    username=username,
                                    keyfile=keyfile,
                                    key=private_key)
        self._client.connect(connection)

    def disconnect(self) -> None:
        self._client.disconnect()

    def exec_command(self, cmd: str) -> RunningCommand:
        if not self.is_connected:
            raise SSHError("Client not connected")

        return self._client.exec_command(cmd)
