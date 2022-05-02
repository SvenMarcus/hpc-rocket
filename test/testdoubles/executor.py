from dataclasses import dataclass, field
from test.slurmoutput import (
    DEFAULT_JOB_ID,
    get_failed_lines,
    get_running_lines,
    get_success_lines,
)
from typing import Callable, List, Optional

from hpcrocket.core.executor import CommandExecutor, RunningCommand

SLURM_SBATCH_COMMAND = "sbatch"
SLURM_SACCT_COMMAND = "sacct -j %s"
SLURM_SCANCEL_COMMAND = "scancel %s"


def is_sbatch(cmd: str) -> bool:
    return cmd.startswith(SLURM_SBATCH_COMMAND)


def is_sacct(cmd: str, jobid: str) -> bool:
    return cmd.startswith(SLURM_SACCT_COMMAND % jobid)


def is_scancel(cmd: str, jobid: str) -> bool:
    return cmd.startswith(SLURM_SCANCEL_COMMAND % jobid)


class CommandExecutorStub(CommandExecutor):
    def __init__(self, command: Optional[RunningCommand] = None) -> None:
        self.command = command or RunningCommandStub()

    def exec_command(self, cmd: str) -> RunningCommand:
        return self.command

    def connect(self) -> None:
        pass

    def close(self) -> None:
        pass


class LoggingCommandExecutorSpy(CommandExecutor):
    @dataclass
    class Command:
        cmd: str
        args: List[str] = field(default_factory=lambda: [])

        def __str__(self) -> str:
            return f"{self.cmd} {' '.join(self.args)}"

    def __init__(self) -> None:
        self.command_log: List[LoggingCommandExecutorSpy.Command] = []
        self.connected = False

    def connect(self) -> None:
        self.connected = True

    def close(self) -> None:
        self.connected = False

    def exec_command(self, cmd: str) -> RunningCommand:
        split = cmd.split()
        self.log_command(split)
        return RunningCommandStub()

    def log_command(self, split: List[str]) -> None:
        self.command_log.append(LoggingCommandExecutorSpy.Command(split[0], split[1:]))


class SlurmJobExecutorSpy(LoggingCommandExecutorSpy):
    def __init__(
        self, sacct_cmd: Optional[RunningCommand] = None, jobid: str = DEFAULT_JOB_ID
    ):
        super().__init__()
        self.sacct_cmd = sacct_cmd or successful_slurm_job_command_stub()
        self.scancel_callback: Callable[[], None] = lambda: None
        self.jobid = jobid

    def on_scancel(self, callback: Callable[[], None]) -> None:
        self.scancel_callback = callback

    def exec_command(self, cmd: str) -> RunningCommand:
        super().exec_command(cmd)
        if is_sbatch(cmd):
            return slurm_job_submitted_command_stub(self.jobid)
        elif is_sacct(cmd, self.jobid):
            return self.sacct_cmd
        elif is_scancel(cmd, self.jobid):
            self.scancel_callback()
            return RunningCommandStub()

        raise ValueError(cmd)


class InfiniteSlurmJobExecutor(LoggingCommandExecutorSpy):
    def exec_command(self, cmd: str) -> RunningCommand:
        super().exec_command(cmd)
        if is_sbatch(cmd):
            return slurm_job_submitted_command_stub(DEFAULT_JOB_ID)

        return running_slurm_job_command_stub()

    def connect(self) -> None:
        pass

    def close(self) -> None:
        pass


class LongRunningSlurmJobExecutorSpy(SlurmJobExecutorSpy):
    def __init__(
        self,
        required_polls_until_done: int = 2,
        jobid: str = DEFAULT_JOB_ID,
        sacct_cmd: Optional[RunningCommand] = None,
    ):
        super().__init__(
            sacct_cmd=sacct_cmd or successful_slurm_job_command_stub(),
            jobid=jobid,
        )
        self.running_commands = iter(
            [running_slurm_job_command_stub()] * required_polls_until_done
        )

    def exec_command(self, cmd: str) -> RunningCommand:
        if not is_sacct(cmd, self.jobid):
            return super().exec_command(cmd)

        super_command = super().exec_command(cmd)
        return next(self.running_commands, super_command)


class RunningCommandStub(RunningCommand):
    def __init__(self, exit_code: int = 0) -> None:
        self.exit_code = exit_code
        self.stdout_lines: List[str] = []
        self.stderr_lines: List[str] = []
        self._waited = False

    @property
    def exit_status(self) -> int:
        self.assert_waited_for_exit()
        return self.exit_code

    def wait_until_exit(self) -> int:
        self._waited = True
        return self.exit_code

    def stderr(self) -> List[str]:
        self.assert_waited_for_exit()
        return self.stderr_lines

    def stdout(self) -> List[str]:
        self.assert_waited_for_exit()
        return self.stdout_lines

    def assert_waited_for_exit(self) -> None:
        assert self._waited, "Did not wait for exit"


def successful_slurm_job_command_stub() -> RunningCommandStub:
    command_stub = RunningCommandStub(exit_code=0)
    command_stub.stdout_lines = get_success_lines()
    return command_stub


def failed_slurm_job_command_stub() -> RunningCommandStub:
    command_stub = RunningCommandStub(exit_code=0)
    command_stub.stdout_lines = get_failed_lines()
    return command_stub


def running_slurm_job_command_stub(exit_code: int = 0) -> RunningCommandStub:
    command_stub = RunningCommandStub(exit_code=exit_code)
    command_stub.stdout_lines = get_running_lines()
    return command_stub


def slurm_job_submitted_command_stub(jobid: str) -> RunningCommandStub:
    command_stub = RunningCommandStub(exit_code=0)
    command_stub.stdout_lines = [f"Submitted Job {jobid}"]
    return command_stub
