from test.slurm_assertions import assert_job_canceled, assert_job_polled
from test.slurmoutput import completed_slurm_job
from test.testdoubles.executor import LoggingCommandExecutorSpy, SlurmJobExecutorSpy

import pytest
from hpcrocket.core.executor import CommandExecutor
from hpcrocket.core.slurmbatchjob import SlurmBatchJob
from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.watcher.jobwatcher import JobWatcher


JOB_ID = "123456"


@pytest.fixture
def executor_spy():
    return SlurmJobExecutorSpy(jobid=JOB_ID)


def make_sut(executor_spy: CommandExecutor, jobid: str) -> SlurmBatchJob:
    slurm = SlurmController(executor_spy)
    return SlurmBatchJob(slurm, jobid)


def test__when_canceling_job__should_execute_scancel_with_executor(executor_spy: LoggingCommandExecutorSpy):
    sut = make_sut(executor_spy, JOB_ID)

    sut.cancel()

    assert_job_canceled(executor_spy, JOB_ID)


def test__given_submitted_job_when_polling_status__should_execute_sacct_with_id(executor_spy: LoggingCommandExecutorSpy):
    sut = make_sut(executor_spy, JOB_ID)

    sut.poll_status()

    assert_job_polled(executor_spy, JOB_ID)


def test__when_polling_status__should_return_job_status(executor_spy: LoggingCommandExecutorSpy):
    sut = make_sut(executor_spy, JOB_ID)

    actual = sut.poll_status()

    assert actual == completed_slurm_job()


def test__given_submitted_job__when_gettings_watcher__should_return_jobwatcher(executor_spy: LoggingCommandExecutorSpy):
    sut = make_sut(executor_spy, JOB_ID)

    actual = sut.get_watcher()

    assert isinstance(actual, JobWatcher)
