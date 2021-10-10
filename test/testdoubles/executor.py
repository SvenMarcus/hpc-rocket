from dataclasses import dataclass, field

from test.slurmoutput import get_error_lines, get_success_lines
from typing import Callable, List
from hpcrocket.core.executor import CommandExecutor, CommandExecutorFactory, RunningCommand


class CommandExecutorFactoryStub(CommandExecutorFactory):

    @classmethod
    def with_slurm_executor_stub(cls, cmd: RunningCommand = None):
        return CommandExecutorFactoryStub(SlurmJobExecutorSpy(cmd))

    @classmethod
    def with_executor_spy(cls):
        return CommandExecutorFactoryStub(CommandExecutorSpy())

    def __init__(self, executor) -> None:
        self._return_value = executor

    def create_executor(self) -> CommandExecutor:
        return self._return_value

class CommandExecutorSpy(CommandExecutor):

    @dataclass
    class Command:
        cmd: str
        args: List[str] = field(default_factory=lambda: [])

        def __str__(self):
            return f"{self.cmd} {' '.join(self.args)}"

    def __init__(self) -> None:
        self.commands: List[CommandExecutorSpy.Command] = []
        self.connected = False

    def connect(self) -> None:
        self.connected = True

    def close(self) -> None:
        self.connected = False

    def exec_command(self, cmd: str) -> RunningCommand:
        split = cmd.split()
        self.commands.append(CommandExecutorSpy.Command(split[0], split[1:]))
        return RunningCommandStub()

class SlurmJobExecutorFactoryStub(CommandExecutorFactory):

    def create_executor(self) -> CommandExecutor:
        return SlurmJobExecutorSpy()


class SlurmJobExecutorSpy(CommandExecutorSpy):

    def __init__(self, sacct_cmd: RunningCommand = None):
        super().__init__()
        self.sacct_cmd = sacct_cmd or CompletedSlurmJobCommandStub()
        self.scancel_callback = lambda: None

    def on_scancel(self, callback: Callable):
        self.scancel_callback = callback        

    def exec_command(self, cmd: str) -> RunningCommand:
        super().exec_command(cmd)
        if cmd.startswith("sbatch"):
            return SlurmJobSubmittedCommandStub()
        elif cmd.startswith("sacct"):
            return self.sacct_cmd
        elif cmd.startswith("scancel"):
            self.scancel_callback()
            return SlurmJobCommandStub()

        raise ValueError(cmd)

    def connect(self) -> None:
        pass

    def close(self) -> None:
        pass


class RunningCommandStub(RunningCommand):

    def __init__(self, exit_code: int = 0) -> None:
        self.exit_code = exit_code

    @property
    def exit_status(self) -> int:
        return self.exit_code

    def wait_until_exit(self) -> int:
        return self.exit_code

    def stderr(self) -> List[str]:
        return None # type: ignore

    def stdout(self) -> List[str]:
        return None # type: ignore


class SlurmJobCommandStub(RunningCommand):

    def wait_until_exit(self) -> int:
        return 0

    @property
    def exit_status(self) -> int:
        return 0

    def stdout(self) -> List[str]:
        return []

    def stderr(self) -> List[str]:
        return []


class InfiniteSlurmJobCommand(SlurmJobCommandStub):

    def __init__(self) -> None:
        self._canceled = False

    def wait_until_exit(self) -> int:
        while not self._canceled:
            continue

        return 0

    def mark_canceled(self):
        self._canceled = True


class CompletedSlurmJobCommandStub(SlurmJobCommandStub):

    def stdout(self) -> List[str]:
        return get_success_lines()


class FailedSlurmJobCommandStub(SlurmJobCommandStub):

    def wait_until_exit(self) -> int:
        return 1

    @property
    def exit_status(self) -> int:
        return 1

    def stdout(self) -> List[str]:
        return get_error_lines()


class SlurmJobSubmittedCommandStub(RunningCommand):

    def wait_until_exit(self) -> int:
        return 0

    @property
    def exit_status(self) -> int:
        return 0

    def stdout(self) -> List[str]:
        return ["Submitted Job 1234"]

    def stderr(self) -> List[str]:
        return []
