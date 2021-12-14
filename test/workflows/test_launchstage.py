from test.application.launchoptions import options
from test.slurm_assertions import (assert_job_canceled, assert_job_polled,
                                   assert_job_submitted)
from test.testdoubles.executor import (DEFAULT_JOB_ID, CommandExecutorStub,
                                       FailedSlurmJobCommandStub,
                                       LongRunningSlurmJobExecutorSpy,
                                       RunningSlurmJobCommandStub,
                                       SlurmJobExecutorSpy)
from typing import List
from unittest.mock import Mock

import pytest
from hpcrocket.core.executor import CommandExecutor, RunningCommand
from hpcrocket.core.launchoptions import LaunchOptions
from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.core.workflows.stages import LaunchStage, NoJobLaunchedError
from hpcrocket.ui import UI
from hpcrocket.watcher.jobwatcher import JobWatcher, JobWatcherFactory


@pytest.fixture
def executor_spy():
    return SlurmJobExecutorSpy()


def run_launch_workflow(options: LaunchOptions, executor: CommandExecutor = None,
                        watcher_factory: JobWatcherFactory = None) -> int:
    executor = executor or SlurmJobExecutorSpy()
    controller = SlurmController(executor, watcher_factory)
    sut = LaunchStage(controller, options)

    return sut(Mock(spec=UI))


def test__given_simple_launchoptions__when_running__should_run_sbatch_with_executor(executor_spy):
    opts = options()
    run_launch_workflow(opts, executor=executor_spy)

    assert_job_submitted(executor_spy, opts.sbatch)


def test__given_launchoptions_with_watching__when_sbatch_job_succeeds__should_return_exit_code_zero():
    actual = run_launch_workflow(options(watch=True))

    assert actual == True


def test__given_options_without_watching__when_running__should_only_sbatch_then_exit(executor_spy):
    opts = options(watch=False)
    run_launch_workflow(opts, executor=executor_spy)

    assert_job_submitted(executor_spy, opts.sbatch)
    assert len(executor_spy.command_log) == 1


def test__given_running_workflow__when_canceling__should_call_cancel_on_job():
    executor = SlurmJobExecutorSpy()
    controller = SlurmController(executor)

    opts = options(watch=True)
    sut = LaunchStage(controller, opts)

    sut(Mock(spec=UI))

    sut.cancel(Mock(spec=UI))

    assert_job_canceled(executor, DEFAULT_JOB_ID, command_index=-1)


def test__when_canceling_before_running__should_raise_no_job_launched_error():
    controller = SlurmController(CommandExecutorStub())
    sut = LaunchStage(controller, options(watch=True))

    with pytest.raises(NoJobLaunchedError):
        sut.cancel(Mock())


class StageCancelingRunningCommand(RunningSlurmJobCommandStub):

    def __init__(self) -> None:
        super().__init__()
        self.sut: LaunchStage = None  # type: ignore

    def wait_until_exit(self) -> int:
        self.sut.cancel(Mock())
        return super().wait_until_exit()
