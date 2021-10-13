from unittest.mock import Mock

from hpcrocket.core.slurmbatchjob import SlurmBatchJob
from hpcrocket.core.slurmcontroller import SlurmController

from test.testdoubles.executor import CommandExecutorSpy, SlurmJobExecutorSpy


def test__when_submitting_job__should_call_sbatch_on_executor():
    executor = SlurmJobExecutorSpy()
    sut = SlurmController(executor)

    jobfile = "jobfile.job"
    sut.submit(jobfile)

    assert_job_submitted(executor, jobfile)


def test__when_submitting_job__should_return_slurm_batch_job():
    executor = SlurmJobExecutorSpy()
    sut = SlurmController(executor)

    jobfile = "jobfile.job"
    actual = sut.submit(jobfile)

    assert isinstance(actual, SlurmBatchJob)


def test__when_submitting_job__job_should_have_jobid():
    jobid = "12345"
    executor = SlurmJobExecutorSpy(jobid=jobid)
    sut = SlurmController(executor)

    jobfile = "jobfile.job"
    actual = sut.submit(jobfile)

    assert actual.jobid == "12345"


def test__when_polling_job__should_call_sacct_on_executor():
    jobid = "12345"
    executor = SlurmJobExecutorSpy(jobid=jobid)
    sut = SlurmController(executor)

    sut.poll_status(jobid)

    assert_job_polled(executor, jobid)


def test__when_polling_job__should_return_job_status():
    jobid = "12345"
    executor = SlurmJobExecutorSpy(jobid=jobid)
    sut = SlurmController(executor)

    actual = sut.poll_status(jobid)

    assert actual == completed_job()


def assert_job_polled(executor: CommandExecutorSpy, jobid: str):
    first_command = executor.commands[0]
    assert first_command.cmd == f"sacct"
    assert first_command.args[:3] == ["-j", jobid]


def assert_job_submitted(executor: CommandExecutorSpy, file: str):
    assert str(executor.commands[0]) == f"sbatch {file}"
