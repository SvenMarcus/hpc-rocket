from dataclasses import dataclass, field
from test.slurmoutput import (DEFAULT_JOB_ID, get_failed_lines,
                              get_running_lines, get_success_lines)
from typing import Callable, List

from hpcrocket.core.executor import CommandExecutor, RunningCommand

SLURM_SBATCH_COMMAND = "sbatch"
SLURM_SACCT_COMMAND = "sacct -j %s"
SLURM_SCANCEL_COMMAND = "scancel %s"


def is_sbatch(cmd: str):
    return cmd.startswith(SLURM_SBATCH_COMMAND)


def is_sacct(cmd: str, jobid: str):
    return cmd.startswith(SLURM_SACCT_COMMAND % jobid)


def is_scancel(cmd: str, jobid: str):
    return cmd.startswith(SLURM_SCANCEL_COMMAND % jobid)


class CommandExecutorStub(CommandExecutor):

    def __init__(self, command: RunningCommand = None) -> None:
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

        def __str__(self):
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

    def log_command(self, split):
        self.command_log.append(LoggingCommandExecutorSpy.Command(split[0], split[1:]))


class SlurmJobExecutorSpy(LoggingCommandExecutorSpy):

    def __init__(self, sacct_cmd: RunningCommand = None, jobid: str = DEFAULT_JOB_ID):
        super().__init__()
        self.sacct_cmd = sacct_cmd or SuccessfulSlurmJobCommandStub()
        self.scancel_callback = lambda: None
        self.jobid = jobid

    def on_scancel(self, callback: Callable):
        self.scancel_callback = callback

    def exec_command(self, cmd: str) -> RunningCommand:
        super().exec_command(cmd)
        if is_sbatch(cmd):
            return SlurmJobSubmittedCommandStub(self.jobid)
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
            return SlurmJobSubmittedCommandStub(DEFAULT_JOB_ID)

        return RunningSlurmJobCommandStub()

    def connect(self) -> None:
        pass

    def close(self) -> None:
        pass


class LongRunningSlurmJobExecutorSpy(SlurmJobExecutorSpy):

    def __init__(self, required_polls_until_done: int = 2,
                 jobid: str = DEFAULT_JOB_ID,
                 sacct_cmd: RunningCommand = None):

        super().__init__(sacct_cmd=sacct_cmd or SuccessfulSlurmJobCommandStub(), jobid=jobid)
        self.running_commands = iter([RunningSlurmJobCommandStub()] * required_polls_until_done)

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

    @property
    def exit_status(self) -> int:
        return self.exit_code

    def wait_until_exit(self) -> int:
        return self.exit_code

    def stderr(self) -> List[str]:
        return self.stderr_lines

    def stdout(self) -> List[str]:
        return self.stdout_lines


class AssertWaitRunningCommandStub(RunningCommandStub):

    def __init__(self, exit_code: int = 0) -> None:
        super().__init__(exit_code=exit_code)
        self._waited = False

    @property
    def exit_status(self) -> int:
        self.assert_waited_for_exit()
        return super().exit_status

    def stderr(self) -> List[str]:
        self.assert_waited_for_exit()
        return super().stderr()

    def stdout(self) -> List[str]:
        self.assert_waited_for_exit()
        return super().stdout()

    def assert_waited_for_exit(self):
        assert self._waited, "Did not wait for exit"

    def wait_until_exit(self) -> int:
        self._waited = True
        return super().exit_status


class SuccessfulSlurmJobCommandStub(AssertWaitRunningCommandStub):

    def __init__(self) -> None:
        super().__init__(exit_code=0)
        self.stdout_lines = get_success_lines()


class FailedSlurmJobCommandStub(AssertWaitRunningCommandStub):

    def __init__(self) -> None:
        super().__init__(exit_code=0)
        self.stdout_lines = get_failed_lines()


class RunningSlurmJobCommandStub(AssertWaitRunningCommandStub):

    def __init__(self, exit_code: int = 0) -> None:
        super().__init__(exit_code=exit_code)
        self.stdout_lines = get_running_lines()


class SlurmJobSubmittedCommandStub(AssertWaitRunningCommandStub):

    def __init__(self, jobid) -> None:
        super().__init__(exit_code=0)
        self.stdout_lines = [f"Submitted Job {jobid}"]
