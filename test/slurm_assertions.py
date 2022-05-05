from test.slurmoutput import DEFAULT_JOB_ID
from test.testdoubles.executor import LoggingCommandExecutorSpy


def assert_job_submitted(executor: LoggingCommandExecutorSpy, file: str) -> None:
    assert str(executor.command_log[0]) == f"sbatch {file}"


def assert_job_polled(
    executor: LoggingCommandExecutorSpy,
    jobid: str = DEFAULT_JOB_ID,
    command_index: int = 0,
) -> None:
    first_command = executor.command_log[command_index]
    _assert_poll_matches(first_command, jobid)


def _assert_poll_matches(cmd: LoggingCommandExecutorSpy.Command, jobid: str) -> None:
    assert cmd.cmd == "sacct"
    assert cmd.args[:2] == ["-j", jobid]


def assert_job_polled_times(
    executor: LoggingCommandExecutorSpy, times: int, jobid: str = DEFAULT_JOB_ID
) -> None:
    total = _count_matching_polls(executor, jobid)
    assert total == times


def _count_matching_polls(executor: LoggingCommandExecutorSpy, jobid: str) -> int:
    total = 0
    for command in executor.command_log:
        if _poll_matches(command, jobid):
            total += 1
    return total


def _poll_matches(command: LoggingCommandExecutorSpy.Command, jobid: str) -> bool:
    return command.cmd == "sacct" and command.args[:2] == ["-j", jobid]


def assert_job_canceled(
    executor: LoggingCommandExecutorSpy,
    jobid: str = DEFAULT_JOB_ID,
    command_index: int = 0,
) -> None:
    first_command = executor.command_log[command_index]
    assert first_command.cmd == "scancel"
    assert first_command.args == [jobid]
