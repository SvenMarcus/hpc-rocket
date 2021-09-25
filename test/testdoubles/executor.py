from test.slurmoutput import get_error_lines, get_success_lines
from typing import List
from hpcrocket.core.executor import CommandExecutor, CommandExecutorFactory, RunningCommand


class SlurmJobExecutorFactoryStub(CommandExecutorFactory):

    def create_executor(self) -> CommandExecutor:
        return SlurmJobExecutorStub()


class SlurmJobExecutorStub(CommandExecutor):

    def __init__(self, sacct_cmd: RunningCommand = None):
        self.sacct_cmd = sacct_cmd or CompletedSlurmJobCommandStub()

    def exec_command(self, cmd: str) -> RunningCommand:
        if cmd.startswith("sbatch"):
            return SlurmJobSubmittedCommandStub()
        elif cmd.startswith("sacct"):
            return self.sacct_cmd

    def close(self) -> None:
        pass


class CompletedSlurmJobCommandStub(RunningCommand):

    def wait_until_exit(self) -> int:
        return 0

    @property
    def exit_status(self) -> int:
        return 0

    def stdout(self) -> List[str]:
        return get_success_lines()

    def stderr(self) -> List[str]:
        return []



class FailedSlurmJobCommandStub(RunningCommand):

    def wait_until_exit(self) -> int:
        return 1

    @property
    def exit_status(self) -> int:
        return 1

    def stdout(self) -> List[str]:
        return get_error_lines()

    def stderr(self) -> List[str]:
        return []


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