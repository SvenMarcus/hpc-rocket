from typing import List, Optional

import paramiko as pm
from paramiko.channel import ChannelFile, ChannelStderrFile, ChannelStdinFile

from hpclaunch.errors import SSHError
from hpclaunch.executor import CommandExecutor, RunningCommand


class RemoteCommand(RunningCommand):
    def __init__(self,
                 stdin: ChannelStdinFile,
                 stdout: ChannelFile,
                 stderr: ChannelStderrFile) -> None:
        self._stdin = stdin
        self._stdout = stdout
        self._stderr = stderr
        self._stdout_lines: List[str] = []
        self._stderr_lines: List[str] = []

    def wait_until_exit(self) -> int:
        while not self._stdout.channel.exit_status_ready():
            continue

        self._stdout_lines = self._stdout.readlines()
        self._stderr_lines = self._stderr.readlines()

        return self._stdout.channel.exit_status

    @property
    def exit_status(self) -> int:
        return self._stdout.channel.exit_status

    def stdout(self) -> List[str]:
        return self._stdout_lines

    def stderr(self) -> List[str]:
        return self._stderr_lines


class SSHExecutor(CommandExecutor):
    def __init__(self, hostname: str) -> None:
        self._hostname: str = hostname
        self._client: pm.SSHClient = pm.SSHClient()

    @property
    def is_connected(self) -> bool:
        transport = self._client.get_transport()
        return transport is not None and transport.is_active()

    def load_host_keys_from_file(self, hostfile: str) -> None:
        self._client.load_host_keys(hostfile)

    def connect(self, username: str, keyfile: Optional[str] = None, password: Optional[str] = None, private_key: Optional[str] = None) -> None:
        try:
            self._client.connect(self._hostname,
                                 username=username, password=password,
                                 key_filename=keyfile, pkey=private_key) # type: ignore[arg-type]
        except Exception as err:
            raise SSHError(str(err))

    def disconnect(self) -> None:
        self._client.close()

    def exec_command(self, cmd: str) -> RemoteCommand:
        if not self.is_connected:
            raise SSHError("Client not connected")

        stdin, stdout, stderr = self._client.exec_command(cmd)
        return RemoteCommand(stdin, stdout, stderr)
