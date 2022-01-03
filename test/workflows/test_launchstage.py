from test.application.launchoptions import launch_options
from test.slurm_assertions import assert_job_canceled, assert_job_submitted
from test.testdoubles.executor import (DEFAULT_JOB_ID, CommandExecutorStub,
                                       SlurmJobExecutorSpy)
from unittest.mock import Mock

import pytest
from hpcrocket.core.executor import CommandExecutor
from hpcrocket.core.launchoptions import LaunchOptions
from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.core.workflows.stages import LaunchStage, NoJobLaunchedError
from hpcrocket.ui import UI
from hpcrocket.watcher.jobwatcher import JobWatcherFactory


@pytest.fixture
def executor_spy():
    return SlurmJobExecutorSpy()


def run_launch_workflow(options: LaunchOptions,
                        executor: CommandExecutor = None,
                        watcher_factory: JobWatcherFactory = None) -> int:
    executor = executor or SlurmJobExecutorSpy()
    controller = SlurmController(executor, watcher_factory)
    sut = LaunchStage(controller, options.sbatch)

    return sut(Mock(spec=UI))


def test__given_simple_launchoptions__when_running__should_run_sbatch_with_executor(executor_spy):
    opts = launch_options()
    run_launch_workflow(opts, executor=executor_spy)

    assert_job_submitted(executor_spy, opts.sbatch)


def test__given_launchoptions__when_running__should_return_true():
    actual = run_launch_workflow(launch_options(watch=True))

    assert actual is True


def test__given_running_workflow__when_canceling__should_call_cancel_on_job():
    executor = SlurmJobExecutorSpy()
    controller = SlurmController(executor)

    opts = launch_options(watch=True)
    sut = LaunchStage(controller, opts)

    sut(Mock(spec=UI))

    sut.cancel(Mock(spec=UI))

    assert_job_canceled(executor, DEFAULT_JOB_ID, command_index=-1)


def test__when_canceling_before_running__should_raise_no_job_launched_error():
    controller = SlurmController(CommandExecutorStub())
    sut = LaunchStage(controller, launch_options(watch=True))

    with pytest.raises(NoJobLaunchedError):
        sut.cancel(Mock())
