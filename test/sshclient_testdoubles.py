from typing import Dict, Iterable, List, Tuple
from ssh_slurm_runner.launchoptions import LaunchOptions


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
        return self.times_called == self._calls_until_done


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

    def __init__(self, cmd_to_channels: Dict[str, ChannelFileStub]):
        self.cmd_to_channels = cmd_to_channels

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


class SSHClientMock(CmdSpecificSSHClientStub):

    def __init__(self, cmd_to_channels: Dict[str, ChannelFileStub], launch_options: LaunchOptions, host_key_file: str):
        super().__init__(cmd_to_channels)
        self._options = launch_options
        self._host_key_file = host_key_file
        self.connected = False
        self.loaded_keys = False
        self.commands = {}

    def connect(self, hostname, port=None, username=None, password=None, pkey=None, key_filename=None, *args, **kwargs):
        assert self.loaded_keys, "Tried to connect without loading known_hosts"
        self.connected = (
            self._options.host == hostname and
            self._options.password == password and
            self._options.user == username and
            self._options.private_key == pkey and
            self._options.private_keyfile == key_filename
        )

    def load_host_keys(self, filename):
        self.loaded_keys = filename == self._host_key_file

    def get_transport(self):
        return TransportStub(self.connected)

    def exec_command(self, command, *args, **kwargs):
        assert self.connected, "Tried to exec_command without being connected"

        self.commands[command.split()[0]] = command
        return super().exec_command(command, *args, **kwargs)

    def close(self):
        self.connected = False

    def verify(self):
        assert self.commands["sbatch"] == f"sbatch {self._options.sbatch}", "Expected: " + \
            f"sbatch {self._options.sbatch}" + f"\nbut was: {self.command}"

        assert not self.connected