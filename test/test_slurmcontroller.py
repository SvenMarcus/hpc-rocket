from test.slurm_assertions import (assert_job_canceled, assert_job_polled,
                                   assert_job_submitted)
from test.slurmoutput import completed_slurm_job
from test.testdoubles.executor import (CommandExecutorStub, RunningCommandStub,
                                       SlurmJobExecutorSpy)
from unittest.mock import Mock

import pytest
from hpcrocket.core.executor import CommandExecutor
from hpcrocket.core.slurmbatchjob import SlurmBatchJob, SlurmError
from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.watcher.jobwatcher import JobWatcher, JobWatcherFactory


def make_sut(executor: CommandExecutor, factory: JobWatcherFactory = None) -> SlurmController:
    return SlurmController(executor, factory)


def test__when_submitting_job__should_call_sbatch_on_executor():
    executor = SlurmJobExecutorSpy()
    sut = make_sut(executor)

    jobfile = "jobfile.job"
    sut.submit(jobfile)

    assert_job_submitted(executor, jobfile)


def test__when_submitting_job__should_return_slurm_batch_job():
    executor = SlurmJobExecutorSpy()
    sut = make_sut(executor)

    jobfile = "jobfile.job"
    actual = sut.submit(jobfile)

    assert isinstance(actual, SlurmBatchJob)


def test__when_submitting_job__job_should_have_jobid():
    jobid = "12345"
    executor = SlurmJobExecutorSpy(jobid=jobid)
    sut = make_sut(executor)

    jobfile = "jobfile.job"
    actual = sut.submit(jobfile)

    assert actual.jobid == "12345"


def test__when_submitting_job_fails__should_raise_slurmerror():
    executor = CommandExecutorStub(RunningCommandStub(exit_code=1))
    sut = make_sut(executor)

    jobfile = "jobfile.job"
    with pytest.raises(SlurmError):
        sut.submit(jobfile)


def test__when_polling_job__should_call_sacct_on_executor():
    jobid = "12345"
    executor = SlurmJobExecutorSpy(jobid=jobid)
    sut = make_sut(executor)

    sut.poll_status(jobid)

    assert_job_polled(executor, jobid)


def test__when_polling_job_fails__should_raise_slurmerror():
    executor = CommandExecutorStub(RunningCommandStub(exit_code=1))

    sut = make_sut(executor)

    jobid = "12345"
    with pytest.raises(SlurmError):
        sut.poll_status(jobid)


def test__when_polling_job__should_return_job_status():
    jobid = "12345"
    executor = SlurmJobExecutorSpy(jobid=jobid)
    sut = make_sut(executor)

    actual = sut.poll_status(jobid)

    assert actual == completed_slurm_job()


def test__when_canceling_job__should_call_scancel_on_executor():
    jobid = "12345"
    executor = SlurmJobExecutorSpy(jobid=jobid)
    sut = make_sut(executor)

    sut.cancel(jobid)

    assert_job_canceled(executor, jobid)


def test__given_watcher_factory__when_submitting_job__should_pass_factory_to_slurm_job():
    executor = SlurmJobExecutorSpy()

    watcher_dummy = Mock(spec=JobWatcher)
    def factory(job):
        return watcher_dummy

    sut = make_sut(executor, factory)
    job = sut.submit("jobfile")

    actual = job.get_watcher()

    assert actual is watcher_dummy



def test__when_canceling_job_fails__should_raise_slurmerror():
    executor = CommandExecutorStub(RunningCommandStub(exit_code=1))
    sut = make_sut(executor)

    jobid = "1234"
    with pytest.raises(SlurmError):
        sut.cancel(jobid)
