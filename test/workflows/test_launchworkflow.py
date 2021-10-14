from test.application.launchoptions import options
from test.testdoubles.executor import (CommandExecutorSpy, FailedSlurmJobCommandStub,
                                       LongRunningSlurmJobExecutorSpy,
                                       SlurmJobExecutorSpy,
                                       SuccessfulSlurmJobCommandStub)
from test.testdoubles.filesystem import DummyFilesystemFactory

from hpcrocket.core.launchoptions import LaunchOptions
from hpcrocket.core.workflows import LaunchWorkflow


def test__given_simple_launchoptions__when_running__should_run_sbatch_with_executor():
    opts = options()
    sut = LaunchWorkflow(DummyFilesystemFactory(), opts)

    executor = SlurmJobExecutorSpy()
    sut.run(executor)

    assert_job_submitted(executor, opts)


def test__given_launchoptions_with_watching__when_sbatch_job_succeeds__should_return_exit_code_zero():
    sut = LaunchWorkflow(DummyFilesystemFactory(), options(watch=True))

    executor = SlurmJobExecutorSpy(sacct_cmd=SuccessfulSlurmJobCommandStub())
    actual = sut.run(executor)

    assert actual == 0


def test__given_launchoptions_with_watching__when_sbatch_job_fails__should_return_exit_code_one():
    sut = LaunchWorkflow(DummyFilesystemFactory(), options(watch=True))

    executor = SlurmJobExecutorSpy(sacct_cmd=FailedSlurmJobCommandStub())
    actual = sut.run(executor)

    assert actual == 1


def test__given_long_running_successful_job__should_poll_job_status_until_finished():
    sut = LaunchWorkflow(DummyFilesystemFactory(), options(watch=True))

    executor = LongRunningSlurmJobExecutorSpy(required_polls_until_done=2)
    actual = sut.run(executor)

    assert_correct_job_poll(executor, command_index=1)
    assert_correct_job_poll(executor, command_index=2)
    assert actual == 0


def test__given_options_without_watching__when_running__should_only_sbatch_then_exit():
    opts = options(watch=False)
    sut = LaunchWorkflow(DummyFilesystemFactory(), opts)

    executor = SlurmJobExecutorSpy()
    sut.run(executor)

    assert_job_submitted(executor, opts)
    assert len(executor.command_log) == 1


def assert_job_submitted(executor: CommandExecutorSpy, opts: LaunchOptions):
    assert str(executor.command_log[0]) == f"sbatch {opts.sbatch}"


def assert_correct_job_poll(executor: CommandExecutorSpy, command_index: int):
    assert executor.command_log[command_index].cmd == "sacct"
    assert executor.command_log[command_index].args[:2] == ["-j", "1234"]
