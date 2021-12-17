from typing import List, Optional

import paramiko as pm
import paramiko.channel as channel
from hpcrocket.core.executor import CommandExecutor, RunningCommand
from hpcrocket.ssh.connectiondata import ConnectionData
from hpcrocket.ssh.errors import SSHError


class RemoteCommand(RunningCommand):
    def __init__(self,
                 stdin: channel.ChannelStdinFile,
                 stdout: channel.ChannelFile,
                 stderr: channel.ChannelStderrFile) -> None:
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

    def __init__(self, connection: ConnectionData, proxyjumps: Optional[List[ConnectionData]] = None) -> None:
        self._is_connected = False
        self._client = _make_sshclient()
        self._connection = connection
        self._proxyjumps = proxyjumps or []

    def load_host_keys_from_file(self, hostfile: str) -> None:
        self._client.load_host_keys(hostfile)

    def connect(self) -> None:
        try:
            channel = build_channel_with_proxyjumps(self._connection, self._proxyjumps)
            _connect_client(self._client, self._connection, channel=channel)
            self._is_connected = True
        except Exception as err:
            raise SSHError(str(err)) from err

    def close(self) -> None:
        self._client.close()
        self._is_connected = False

    def exec_command(self, command: str) -> RunningCommand:
        stdin, stdout, stderr = self._client.exec_command(command)
        return RemoteCommand(stdin, stdout, stderr)

    @property
    def is_connected(self) -> bool:
        return self._is_connected

    @property
    def client(self) -> pm.SSHClient:
        return self._client


def build_channel_with_proxyjumps(connection: ConnectionData, proxyjumps: List[ConnectionData]) -> Optional[pm.Channel]:
    channel = None
    for index, proxyjump in enumerate(proxyjumps):
        next_host = _next_host(connection, proxyjumps, index)
        proxy = _make_sshclient_and_connect(proxyjump, channel)
        channel = _open_channel_to_next_host(next_host, proxy)

    return channel


def _next_host(connection: ConnectionData, proxyjumps: List[ConnectionData], index: int) -> ConnectionData:
    if index < len(proxyjumps) - 1:
        return proxyjumps[index + 1]

    return connection


def _open_channel_to_next_host(next_connection: ConnectionData, proxy: pm.SSHClient) -> pm.Channel:
    transport = proxy.get_transport()
    channel = transport.open_channel(  # type: ignore
        "direct-tcpip", (next_connection.hostname, next_connection.port), ('', 0))

    return channel


def _make_sshclient_and_connect(connection: ConnectionData, channel: Optional[pm.Channel] = None) -> pm.SSHClient:
    sshclient = _make_sshclient()
    _connect_client(sshclient, connection, channel)
    return sshclient


def _make_sshclient() -> pm.SSHClient:
    sshclient = pm.SSHClient()
    sshclient.set_missing_host_key_policy(pm.AutoAddPolicy)
    return sshclient


def _connect_client(sshclient: pm.SSHClient, connection: ConnectionData, channel: Optional[pm.Channel]) -> None:
    sshclient.connect(
        hostname=connection.hostname,
        username=connection.username,
        port=connection.port,
        key_filename=connection.keyfile,
        password=connection.password,
        pkey=connection.key,  # type: ignore[arg-type]
        sock=channel  # type: ignore[arg-type]
    )
