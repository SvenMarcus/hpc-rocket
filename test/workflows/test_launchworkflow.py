from hpcrocket.core.slurmcontroller import SlurmController
from test.application.launchoptions import options
from test.testdoubles.executor import (FailedSlurmJobCommandStub,
                                       LongRunningSlurmJobExecutorSpy,
                                       SlurmJobExecutorSpy,
                                       SuccessfulSlurmJobCommandStub)
from test.slurm_assertions import assert_job_submitted, assert_job_polled, assert_job_canceled
from test.testdoubles.filesystem import DummyFilesystemFactory

from hpcrocket.core.workflows import LaunchWorkflow


def test__given_simple_launchoptions__when_running__should_run_sbatch_with_executor():
    opts = options()
    sut = LaunchWorkflow(DummyFilesystemFactory(), opts)

    executor = SlurmJobExecutorSpy()
    controller = SlurmController(executor)
    sut.run(controller)

    assert_job_submitted(executor, opts.sbatch)


def test__given_launchoptions_with_watching__when_sbatch_job_succeeds__should_return_exit_code_zero():
    sut = LaunchWorkflow(DummyFilesystemFactory(), options(watch=True))

    executor = SlurmJobExecutorSpy(sacct_cmd=SuccessfulSlurmJobCommandStub())
    controller = SlurmController(executor)
    actual = sut.run(controller)

    assert actual == 0


def test__given_launchoptions_with_watching__when_sbatch_job_fails__should_return_exit_code_one():
    sut = LaunchWorkflow(DummyFilesystemFactory(), options(watch=True))

    executor = SlurmJobExecutorSpy(sacct_cmd=FailedSlurmJobCommandStub())
    controller = SlurmController(executor)
    actual = sut.run(controller)

    assert actual == 1


def test__given_long_running_successful_job__should_poll_job_status_until_finished():
    sut = LaunchWorkflow(DummyFilesystemFactory(), options(watch=True))

    executor = LongRunningSlurmJobExecutorSpy(required_polls_until_done=2)
    controller = SlurmController(executor)
    actual = sut.run(controller)

    assert_job_polled(executor, command_index=1)
    assert_job_polled(executor, command_index=2)
    assert actual == 0


def test__given_options_without_watching__when_running__should_only_sbatch_then_exit():
    opts = options(watch=False)
    sut = LaunchWorkflow(DummyFilesystemFactory(), opts)

    executor = SlurmJobExecutorSpy()
    controller = SlurmController(executor)
    sut.run(controller)

    assert_job_submitted(executor, opts.sbatch)
    assert len(executor.command_log) == 1
