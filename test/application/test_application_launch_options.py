from test.application.executor_filesystem_callorder import (
    CallOrderVerification, VerifierReturningFilesystemFactory)
from test.application.launchoptions import launch_options, main_connection
from test.slurmoutput import completed_slurm_job
from test.testdoubles.executor import (FailedSlurmJobCommandStub,
                                       LoggingCommandExecutorSpy,
                                       SlurmJobExecutorSpy,
                                       SuccessfulSlurmJobCommandStub)
from test.testdoubles.filesystem import DummyFilesystemFactory
from unittest.mock import Mock

import pytest
from hpcrocket.core.application import Application
from hpcrocket.core.environmentpreparation import CopyInstruction
from hpcrocket.core.executor import RunningCommand
from hpcrocket.ssh.errors import SSHError


class ConnectionFailingCommandExecutor(LoggingCommandExecutorSpy):

    def connect(self) -> None:
        raise SSHError(main_connection().hostname)

    def close(self):
        pass

    def exec_command(self, cmd: str) -> RunningCommand:
        pass


def make_sut(executor, ui=None):
    return Application(executor, DummyFilesystemFactory(), ui or Mock())


def make_sut_with_call_order_verification(expected_calls):
    verifier = CallOrderVerification(expected_calls)
    factory = VerifierReturningFilesystemFactory(verifier)
    sut = Application(verifier, factory, Mock())
    return sut, verifier


def test__given_valid_config__when_running__should_run_sbatch_with_executor():
    executor = SlurmJobExecutorSpy()
    sut = make_sut(executor)

    sut.run(launch_options())

    assert str(executor.command_log[0]) == f"sbatch {launch_options().sbatch}"


def test__given_valid_config__when_sbatch_job_succeeds__should_return_exit_code_zero():
    executor = SlurmJobExecutorSpy(sacct_cmd=SuccessfulSlurmJobCommandStub())
    sut = make_sut(executor)

    actual = sut.run(launch_options(watch=True))

    assert actual == 0


def test__given_valid_config__when_sbatch_job_fails__should_return_exit_code_one():
    executor = SlurmJobExecutorSpy(sacct_cmd=FailedSlurmJobCommandStub())
    sut = make_sut(executor)

    actual = sut.run(launch_options(watch=True))

    assert actual == 1


def test__given_ui__when_running__should_update_ui_after_polling():
    ui_spy = Mock()
    executor = SlurmJobExecutorSpy()
    sut = make_sut(executor, ui_spy)

    _ = sut.run(launch_options(watch=True))

    ui_spy.update.assert_called_with(completed_slurm_job())


def test__given_failing_ssh_connection__when_running__should_log_error_and_exit_with_code_1():
    ui_spy = Mock()

    executor = ConnectionFailingCommandExecutor()
    sut = make_sut(executor, ui_spy)

    actual = sut.run(launch_options(watch=True))

    ui_spy.error.assert_called_once_with(
        f"SSHError: {main_connection().hostname}")
    assert executor.command_log == []
    assert actual == 1


def test__given_options_without_watch_and_files_to_copy_collect_and_clean__when_running__should_first_copy_to_remote_then_execute_job_then_exit():
    opts = launch_options(
        copy=[CopyInstruction("myfile.txt", "mycopy.txt")],
        collect=[CopyInstruction("mycopy.txt", "mycollect.txt")],
        clean=["mycopy.txt"],
        watch=False
    )

    expected = [
        "copy myfile.txt mycopy.txt",
        "sbatch"
    ]

    sut, verify = make_sut_with_call_order_verification(expected)

    sut.run(opts)

    verify()


def test__given_launchoptions_with_watch_and_files_to_copy_collect_and_clean__when_running__should_first_copy_to_remote_then_execute_job_then_collect_then_clean():
    opts = launch_options(
        copy=[CopyInstruction("myfile.txt", "mycopy.txt")],
        collect=[CopyInstruction("mycopy.txt", "mycollect.txt")],
        clean=["mycopy.txt"],
        watch=True
    )

    expected = [
        "copy myfile.txt mycopy.txt",
        "sbatch",
        "sacct",
        "copy mycopy.txt mycollect.txt",
        "delete mycopy.txt",
    ]

    sut, verify = make_sut_with_call_order_verification(expected)

    sut.run(opts)

    verify()
