from test.slurmoutput import completed_slurm_job
from test.testdoubles.executor import CommandExecutorSpy, CommandExecutorStub, FailedSlurmJobCommandStub, RunningCommandStub, SlurmJobExecutorSpy

import pytest
from hpcrocket.core.slurmbatchjob import SlurmBatchJob, SlurmError
from hpcrocket.core.slurmcontroller import SlurmController


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


def test__when_submitting_job_fails__should_raise_slurmerror():
    executor = CommandExecutorStub(RunningCommandStub(exit_code=1))
    sut = SlurmController(executor)

    jobfile = "jobfile.job"
    with pytest.raises(SlurmError):
        sut.submit(jobfile)


def test__when_polling_job__should_call_sacct_on_executor():
    jobid = "12345"
    executor = SlurmJobExecutorSpy(jobid=jobid)
    sut = SlurmController(executor)

    sut.poll_status(jobid)

    assert_job_polled(executor, jobid)


def test__when_polling_job_fails__should_raise_slurmerror():
    executor = CommandExecutorStub(RunningCommandStub(exit_code=1))

    sut = SlurmController(executor)

    jobid = "12345"
    with pytest.raises(SlurmError):
        sut.poll_status(jobid)


def test__when_polling_job__should_return_job_status():
    jobid = "12345"
    executor = SlurmJobExecutorSpy(jobid=jobid)
    sut = SlurmController(executor)

    actual = sut.poll_status(jobid)

    assert actual == completed_slurm_job()


def test__when_canceling_job__should_call_scancel_on_executor():
    jobid = "12345"
    executor = SlurmJobExecutorSpy(jobid=jobid)
    sut = SlurmController(executor)

    sut.cancel(jobid)

    assert_job_canceled(executor, jobid)


def test__when_canceling_job_fails__should_raise_slurmerror():
    executor = CommandExecutorStub(RunningCommandStub(exit_code=1))
    sut = SlurmController(executor)

    jobid = "1234"
    with pytest.raises(SlurmError):
        sut.cancel(jobid)


def assert_job_submitted(executor: CommandExecutorSpy, file: str):
    assert str(executor.command_log[0]) == f"sbatch {file}"


def assert_job_polled(executor: CommandExecutorSpy, jobid: str):
    first_command = executor.command_log[0]
    assert first_command.cmd == f"sacct"
    assert first_command.args[:2] == ["-j", jobid]


def assert_job_canceled(executor: CommandExecutorSpy, jobid: str):
    first_command = executor.command_log[0]
    assert first_command.cmd == "scancel"
    assert first_command.args == [jobid]
