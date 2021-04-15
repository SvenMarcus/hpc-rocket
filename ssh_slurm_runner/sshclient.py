from ssh_slurm_runner.executor import CommandExecutor, RunningCommand
import paramiko as pm


class RemoteCommand(RunningCommand):
    def __init__(self, stdin, stdout, stderr) -> None:
        self._stdin = stdin
        self._stdout = stdout
        self._stderr = stderr
        self._stdout_lines = []
        self._stderr_lines = []

    def wait_until_exit(self) -> int:
        while not self._stdout.channel.exit_status_ready():
            continue

        self._stdout_lines = self._stdout.readlines()
        self._stderr_lines = self._stderr.readlines()

        return self._stdout.channel.exit_status

    def is_done(self) -> bool:
        return self._stdout.channel.exit_status_ready()

    @property
    def exit_status(self) -> int:
        return self._stdout.channel.exit_status

    def stdout(self) -> str:
        return self._stdout_lines

    def stderr(self) -> str:
        return self._stderr_lines


class SSHClient(CommandExecutor):
    def __init__(self, hostname: str) -> None:
        self._hostname: str = hostname
        self._client: pm.SSHClient = pm.SSHClient()
        self._client_connected = False

    @property
    def is_connected(self) -> bool:
        transport = self._client.get_transport()
        return transport is not None and transport.is_active()

    def connect(self, username: str, keyfile: str, known_hosts_file: str = None) -> None:
        self._client.load_host_keys(known_hosts_file)
        self._client.connect(
            self._hostname, username=username, key_filename=keyfile)

    def disconnect(self) -> None:
        self._client.close()

    def exec_command(self, cmd: str) -> RemoteCommand:
        if not self.is_connected:
            raise pm.SSHException("Client not connected")

        stdin, stdout, stderr = self._client.exec_command(cmd)
        return RemoteCommand(stdin, stdout, stderr)
