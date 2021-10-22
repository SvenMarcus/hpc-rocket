from test.application.launchoptions import options
from test.slurm_assertions import assert_job_polled, assert_job_submitted
from test.testdoubles.executor import (FailedSlurmJobCommandStub,
                                       LongRunningSlurmJobExecutorSpy,
                                       SlurmJobExecutorSpy)


import pytest

from hpcrocket.core.executor import CommandExecutor
from hpcrocket.core.launchoptions import LaunchOptions
from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.core.workflows.stages import LaunchStage


@pytest.fixture
def executor_spy():
    return SlurmJobExecutorSpy()


def run_launch_workflow(options: LaunchOptions, executor: CommandExecutor = None) -> int:
    executor = executor or SlurmJobExecutorSpy()
    controller = SlurmController(executor)
    sut = LaunchStage(controller, options)

    return sut()


def test__given_simple_launchoptions__when_running__should_run_sbatch_with_executor(executor_spy):
    opts = options()
    run_launch_workflow(opts, executor=executor_spy)

    assert_job_submitted(executor_spy, opts.sbatch)


def test__given_launchoptions_with_watching__when_sbatch_job_succeeds__should_return_exit_code_zero():
    actual = run_launch_workflow(options(watch=True))

    assert actual == True


def test__given_launchoptions_with_watching__when_sbatch_job_fails__should_return_exit_code_one():
    failing_cmd_executor = SlurmJobExecutorSpy(sacct_cmd=FailedSlurmJobCommandStub())
    actual = run_launch_workflow(options(watch=True), executor=failing_cmd_executor)

    assert actual == False


def test__given_long_running_successful_job__should_poll_job_status_until_finished():
    executor_spy = LongRunningSlurmJobExecutorSpy(required_polls_until_done=2)
    actual = run_launch_workflow(options(watch=True), executor=executor_spy)

    assert_job_polled(executor_spy, command_index=1)
    assert_job_polled(executor_spy, command_index=2)
    assert actual == True


def test__given_options_without_watching__when_running__should_only_sbatch_then_exit(executor_spy):
    opts = options(watch=False)
    run_launch_workflow(opts, executor=executor_spy)

    assert_job_submitted(executor_spy, opts.sbatch)
    assert len(executor_spy.command_log) == 1
