import pytest

from test.application.launchoptions import main_connection, options
from test.testdoubles.executor import (CommandExecutorFactoryStub,
                                       CommandExecutorSpy,
                                       FailedSlurmJobCommandStub,
                                       InfiniteSlurmJobCommand,
                                       SlurmJobExecutorSpy)
from test.testdoubles.filesystem import DummyFilesystemFactory
from unittest.mock import Mock

from hpcrocket.core.application import Application
from hpcrocket.core.executor import RunningCommand
from hpcrocket.core.slurmbatchjob import SlurmJobStatus, SlurmTaskStatus
from hpcrocket.ssh.errors import SSHError


class ConnectionFailingCommandExecutor(CommandExecutorSpy):

    def connect(self) -> None:
        raise SSHError(main_connection().hostname)

    def close(self):
        pass

    def exec_command(self, cmd: str) -> RunningCommand:
        pass


def test__given_failing_ssh_connection__when_running__should_log_error_and_exit_with_code_1():
    ui_spy = Mock()

    executor = ConnectionFailingCommandExecutor()
    sut = Application(CommandExecutorFactoryStub(executor), DummyFilesystemFactory(), ui_spy)

    actual = sut.run(options(watch=True))

    ui_spy.error.assert_called_once_with(f"SSHError: {main_connection().hostname}")
    assert executor.commands == []
    assert actual == 1


def test__given_valid_config__when_sbatch_job_succeeds__should_return_exit_code_zero():
    executor = SlurmJobExecutorSpy()
    sut = Application(CommandExecutorFactoryStub(executor), DummyFilesystemFactory(), Mock())

    actual = sut.run(options(watch=True))

    assert actual == 0


def test__given_valid_config__when_sbatch_job_fails__should_return_exit_code_one():
    executor = SlurmJobExecutorSpy(sacct_cmd=FailedSlurmJobCommandStub())
    sut = Application(CommandExecutorFactoryStub(executor), DummyFilesystemFactory(), Mock())

    actual = sut.run(options(watch=True))

    assert actual == 1


def test__given_ui__when_running__should_update_ui_after_polling():
    ui_spy = Mock()
    factory = CommandExecutorFactoryStub.with_slurm_executor_stub()
    sut = Application(factory, DummyFilesystemFactory(), ui_spy)

    _ = sut.run(options(watch=True))

    ui_spy.update.assert_called_with(completed_slurm_job())


@pytest.mark.timeout(1)
def test__given_infinite_running_job__when_canceling__should_cancel_job_and_exit_with_code_130():
    infinite_running_job = InfiniteSlurmJobCommand()
    executor = SlurmJobExecutorSpy(sacct_cmd=infinite_running_job)
    executor.on_scancel(infinite_running_job.mark_canceled)

    sut = Application(CommandExecutorFactoryStub(executor), DummyFilesystemFactory(), Mock())
    thread = run_in_background(sut)

    wait_until_polled(executor)
    actual = sut.cancel()

    thread.join()
    assert actual == 130


def run_in_background(sut):
    from threading import Thread
    thread = Thread(target=lambda: sut.run(options(watch=True)))
    thread.start()

    return thread


def wait_until_polled(executor: CommandExecutorSpy):
    def was_polled():
        return any(logged_command.cmd.startswith("sacct")
                   for logged_command in executor.commands)

    while not was_polled():
        continue


def completed_slurm_job():
    return SlurmJobStatus(
        id="1603353",
        name="PyFluidsTest",
        state="COMPLETED",
        tasks=[
            SlurmTaskStatus("1603353", "PyFluidsTest", "COMPLETED"),
            SlurmTaskStatus("1603353.bat+", "batch", "COMPLETED"),
            SlurmTaskStatus("1603353.ext+",  "extern", "COMPLETED"),
            SlurmTaskStatus("1603353.0", "singularity", "COMPLETED"),
            SlurmTaskStatus("1603353.1", "singularity", "COMPLETED"),
            SlurmTaskStatus("1603353.2", "singularity", "COMPLETED"),
            SlurmTaskStatus("1603353.3", "singularity", "COMPLETED")
        ]
    )
