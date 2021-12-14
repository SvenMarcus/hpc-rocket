from dataclasses import dataclass
from test.slurmoutput import get_success_lines
from typing import Dict, List, Optional, Tuple, Type
from unittest.mock import Mock

from hpcrocket.core.launchoptions import LaunchOptions
from hpcrocket.ssh.connectiondata import ConnectionData


class TransportStub:

    def __init__(self, active: bool) -> None:
        self._active = active

    def is_active(self):
        return self._active


class ChannelStub:

    def __init__(self, exit_code: int = 0, exit_code_ready: bool = True):
        self._exit_code = exit_code
        self._code_ready = exit_code_ready

    @property
    def exit_status(self):
        return self._exit_code

    def exit_status_ready(self):
        return self._code_ready


class DelayedChannelSpy(ChannelStub):

    def __init__(self, exit_code: int = 0, calls_until_exit: int = 0):
        super().__init__(exit_code)
        self._calls_until_done = calls_until_exit
        self.times_called = 0

    def exit_status_ready(self):
        self.times_called += 1
        return self.times_called >= self._calls_until_done


class ChannelFileStub:

    def __init__(self, lines: List[str] = None, channel: ChannelStub = None):
        self._lines = lines or []
        self._channel = channel

    @property
    def channel(self):
        return self._channel or ChannelStub()

    def readlines(self):
        return self._lines


class CmdSpecificSSHClientStub:

    @classmethod
    def successful(cls: Type['CmdSpecificSSHClientStub']):
        return SuccessfulSlurmCmdSSHClient()


    def __init__(self, cmd_to_channels: Dict[str, ChannelFileStub]):
        self.cmd_to_channels = cmd_to_channels

    def set_missing_host_key_policy(self, *args):
        pass

    def connect(self, hostname, port=None, username=None, password=None, pkey=None, key_filename=None, *args, **kwargs):
        pass

    def load_host_keys(self, filename):
        pass

    def get_transport(self):
        return TransportStub(True)

    def exec_command(self, command: str, *args, **kwargs):
        split_cmd = command.split()
        actual_cmd = split_cmd[0]
        return ChannelFileStub(), self.cmd_to_channels[actual_cmd], ChannelFileStub()

    def close(self):
        pass


class SuccessfulSlurmCmdSSHClient(CmdSpecificSSHClientStub):

    def __init__(self):
        super().__init__({
            "sbatch": ChannelFileStub(lines=["1234"]),
            "sacct": ChannelFileStub(lines=get_success_lines())
        })


class SSHClientMock(SuccessfulSlurmCmdSSHClient):

    def __init__(self, launch_options: LaunchOptions, private_keyfile_abspath: str = None):
        super().__init__()
        self._options = launch_options
        self._private_keyfile_abspath = private_keyfile_abspath or launch_options.connection.keyfile
        self.connected = False
        self.commands: Dict[str, ChannelFileStub] = {}

    def connect(self, hostname, port=None, username=None, password=None, pkey=None, key_filename=None, *args, **kwargs):
        self.connected = (
            self._options.connection.hostname == hostname and
            self._options.connection.password == password and
            self._options.connection.username == username and
            self._options.connection.port == port and
            self._options.connection.key == pkey and
            self._private_keyfile_abspath == key_filename
        )

    def load_host_keys(self, filename):
        pass

    def get_transport(self):
        return TransportStub(self.connected)

    def exec_command(self, command, *args, **kwargs):
        assert self.connected, "Tried to exec_command without being connected"

        self.commands[command.split()[0]] = command
        return super().exec_command(command, *args, **kwargs)

    def close(self):
        self.connected = False

    def verify(self):
        assert self.commands["sbatch"] == f"sbatch {self._options.sbatch}", \
            "Expected: " + f"sbatch {self._options.sbatch}" + f"\nbut was: {self.commands['sbatch']}"

        assert not self.connected


class ProxyJumpVerifyingSSHClient(SuccessfulSlurmCmdSSHClient):

    @dataclass
    class ChannelMock:
        kind: str
        dest_addr: Optional[Tuple] = None
        src_addr: Optional[Tuple] = None
        window_size: Optional[int] = None
        max_packet_size: Optional[int] = None
        timeout: Optional[int] = None

        def assert_points_to(self, host: ConnectionData):
            assert self.kind == "direct-tcpip"
            assert self.dest_addr == (host.hostname, host.port)
            assert self.src_addr == ('', 0)

    class TransportStub:

        def __getattr__(self, name):
            return Mock(name)

        def open_channel(self, *args, **kwargs):
            return ProxyJumpVerifyingSSHClient.ChannelMock(*args, **kwargs)

    def __init__(self, connection: ConnectionData, proxyjumps: List[ConnectionData]) -> None:
        super().__init__()
        self.expected_path = [*proxyjumps, connection]
        self.recorded_connection_path: List[ConnectionData] = []
        self._recorded_channels: List[ProxyJumpVerifyingSSHClient.ChannelMock] = []

    def connect(self, hostname, port=None, username=None, password=None, pkey=None, key_filename=None, *args, **kwargs):
        self.recorded_connection_path.append(ConnectionData(hostname, username, password, key_filename, pkey, port))
        self._recorded_channels.append(kwargs.get("sock"))

    def get_transport(self):
        return ProxyJumpVerifyingSSHClient.TransportStub()

    def verify(self):
        self._assert_all_nodes_connected_in_order()
        self._assert_channels_built_in_order()

    def _assert_all_nodes_connected_in_order(self):
        assert self.expected_path == self.recorded_connection_path

    def _assert_channels_built_in_order(self):
        no_channel = self._recorded_channels.pop(0)
        assert no_channel is None, "First connection should not have a channel"

        path_from_2nd_proxy_to_end = self.expected_path[1:]
        channels_and_proxies = zip(self._recorded_channels, path_from_2nd_proxy_to_end)
        for nth_channel, n_plus_1th_proxy in channels_and_proxies:
            nth_channel.assert_points_to(n_plus_1th_proxy)
