from test.application.launchoptions import main_connection
from test.testdoubles.executor import CommandExecutorFactoryStub, FailedSlurmJobCommandStub, InfiniteSlurmJobCommand, RunningCommandStub, SlurmJobExecutorFactoryStub, SlurmJobExecutorSpy
from test.testdoubles.filesystem import DummyFilesystemFactory
from unittest.mock import Mock, patch

from hpcrocket.core.application import Application
from hpcrocket.core.launchoptions import JobBasedOptions


def test__given_job_options_with_status_action__when_running__should_poll_job_status_once_and_exit():
    opts = JobBasedOptions(
        jobid="1234",
        action=JobBasedOptions.Action.status,
        connection=main_connection()
    )

    executor = SlurmJobExecutorSpy(sacct_cmd=InfiniteSlurmJobCommand())
    factory = CommandExecutorFactoryStub(executor)
    sut = Application(factory, DummyFilesystemFactory(), Mock())

    actual = sut.run(opts)

    assert_sacct_executed_with_jobid(executor)
    assert actual == 0


def test__when_status_poll_fails__should_exit_with_code_1():
    opts = JobBasedOptions(
        jobid="1234",
        action=JobBasedOptions.Action.status,
        connection=main_connection()
    )

    executor = SlurmJobExecutorSpy(sacct_cmd=RunningCommandStub(exit_code=1))
    factory = CommandExecutorFactoryStub(executor)
    sut = Application(factory, DummyFilesystemFactory(), Mock())

    actual = sut.run(opts)

    assert actual == 1



def assert_sacct_executed_with_jobid(executor: SlurmJobExecutorSpy):
    sacct_cmd = executor.commands[0]
    
    assert sacct_cmd.cmd == "sacct"
    assert sacct_cmd.args[:2] == ["-j", "1234"]
