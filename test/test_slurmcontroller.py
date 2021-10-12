from unittest.mock import Mock

from hpcrocket.core.slurmbatchjob import SlurmBatchJob
from hpcrocket.core.slurmcontroller import SlurmController

from test.testdoubles.executor import CommandExecutorSpy


def test__when_submitting_job__should_call_sbatch_on_executor():
    executor = CommandExecutorSpy()
    sut = SlurmController(executor)

    jobfile = "jobfile.job"
    sut.submit(jobfile)

    assert_job_submitted(executor, jobfile)


def test__when_submitting_job__should_return_slurm_batch_job():
    executor = Mock()
    sut = SlurmController(executor)

    jobfile = "jobfile.job"
    actual = sut.submit(jobfile)

    assert isinstance(actual, SlurmBatchJob)


def assert_job_submitted(executor: CommandExecutorSpy, file: str):
    assert str(executor.commands[0]) == f"sbatch {file}"
