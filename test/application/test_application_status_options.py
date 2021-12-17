from test.application.executor_filesystem_callorder import (
    CallOrderVerification, VerifierReturningFilesystemFactory)
from test.application.launchoptions import main_connection
from test.slurm_assertions import assert_job_polled
from test.slurmoutput import DEFAULT_JOB_ID, completed_slurm_job
from test.testdoubles.executor import (AssertWaitRunningCommandStub,
                                       RunningCommandStub, SlurmJobExecutorSpy,
                                       SuccessfulSlurmJobCommandStub)
from test.testdoubles.filesystem import DummyFilesystemFactory
from unittest.mock import Mock

import pytest
from hpcrocket.core.application import Application
from hpcrocket.core.launchoptions import SimpleJobOptions


@pytest.fixture
def options():
    return SimpleJobOptions(
        jobid=DEFAULT_JOB_ID,
        action=SimpleJobOptions.Action.status,
        connection=main_connection()
    )


def make_sut(executor, ui=None):
    return Application(executor, DummyFilesystemFactory(), ui or Mock())


def test__given_job_options_with_status_action__when_running__should_poll_job_status_once_and_exit(options):
    executor = SlurmJobExecutorSpy(sacct_cmd=AssertWaitRunningCommandStub())
    sut = make_sut(executor)

    actual = sut.run(options)

    assert_job_polled(executor)
    assert actual == 0


def test__given_job_option_with_status_action__when_running__should_update_ui_with_job_status(options):
    executor = SlurmJobExecutorSpy(sacct_cmd=SuccessfulSlurmJobCommandStub())

    ui_spy = Mock()
    sut = make_sut(executor, ui_spy)

    sut.run(options)

    ui_spy.update.assert_called_with(completed_slurm_job())


def test__when_status_poll_fails__should_exit_with_code_1(options):
    executor = SlurmJobExecutorSpy(sacct_cmd=RunningCommandStub(exit_code=1))
    sut = make_sut(executor, Mock())

    actual = sut.run(options)

    assert actual == 1


def test__given_job_options_with_status_action__should_only_poll_job_status(options):
    verifier = CallOrderVerification(["sacct"])
    factory = VerifierReturningFilesystemFactory(verifier)

    sut = Application(verifier, factory, Mock())

    sut.run(options)

    factory.verifier()
