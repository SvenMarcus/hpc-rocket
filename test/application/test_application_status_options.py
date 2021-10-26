import pytest

from test.application.executor_filesystem_callorder import CallOrderVerificationFactory
from test.application.launchoptions import main_connection
from test.testdoubles.executor import CommandExecutorFactoryStub, SuccessfulSlurmJobCommandStub, AssertWaitRunningCommandStub, InfiniteSlurmJobCommand, RunningCommandStub, SlurmJobExecutorFactoryStub, SlurmJobExecutorSpy
from test.testdoubles.filesystem import DummyFilesystemFactory
from test.slurmoutput import DEFAULT_JOB_ID, completed_slurm_job
from test.slurm_assertions import assert_job_polled
from unittest.mock import Mock

from hpcrocket.core.application import Application
from hpcrocket.core.launchoptions import JobBasedOptions


@pytest.fixture
def options():
    return JobBasedOptions(
        jobid=DEFAULT_JOB_ID,
        action=JobBasedOptions.Action.status,
        connection=main_connection()
    )


def test__given_job_options_with_status_action__when_running__should_poll_job_status_once_and_exit(options):
    executor = SlurmJobExecutorSpy(sacct_cmd=AssertWaitRunningCommandStub())
    factory = CommandExecutorFactoryStub(executor)
    sut = Application(factory, DummyFilesystemFactory(), Mock())

    actual = sut.run(options)

    assert_job_polled(executor)
    assert actual == 0


def test__given_job_option_with_status_action__when_running__should_update_ui_with_job_status(options):
    executor = SlurmJobExecutorSpy(sacct_cmd=SuccessfulSlurmJobCommandStub())
    factory = CommandExecutorFactoryStub(executor)

    ui_spy = Mock()
    sut = Application(factory, DummyFilesystemFactory(), ui_spy)

    sut.run(options)

    ui_spy.update.assert_called_with(completed_slurm_job())


def test__when_status_poll_fails__should_exit_with_code_1(options):
    executor = SlurmJobExecutorSpy(sacct_cmd=RunningCommandStub(exit_code=1))
    factory = CommandExecutorFactoryStub(executor)
    sut = Application(factory, DummyFilesystemFactory(), Mock())

    actual = sut.run(options)

    assert actual == 1


def test__given_job_options_with_status_action__should_only_poll_job_status():
    opts = JobBasedOptions(
        jobid="1234",
        action=JobBasedOptions.Action.status,
        connection=main_connection()
    )

    factory = CallOrderVerificationFactory()
    factory.verifier.expected = ["sacct"]

    sut = Application(factory, factory, Mock())

    sut.run(opts)

    factory.verifier()
