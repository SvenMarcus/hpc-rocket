from test.application.launchoptions import options
from test.testdoubles.executor import (CommandExecutorSpy, FailedSlurmJobCommandStub,
                                       LongRunningSlurmJobExecutorSpy,
                                       SlurmJobExecutorSpy,
                                       SuccessfulSlurmJobCommandStub)
from test.testdoubles.filesystem import DummyFilesystemFactory

from hpcrocket.core.workflows import LaunchWorkflow


def test__given_simple_launchoptions__when_running__should_run_sbatch_with_executor():
    opts = options()
    sut = LaunchWorkflow(DummyFilesystemFactory(), opts)

    executor = SlurmJobExecutorSpy()
    sut.run(executor)

    assert str(executor.commands[0]) == f"sbatch {opts.sbatch}"


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


def test__given_long_running_job__should_poll_job_status_until_finished():
    sut = LaunchWorkflow(DummyFilesystemFactory(), options(watch=True))

    executor = LongRunningSlurmJobExecutorSpy()
    sut.run(executor)

    assert_correct_job_poll(executor, command_index=1)
    assert_correct_job_poll(executor, command_index=2)


def assert_correct_job_poll(executor: CommandExecutorSpy, command_index: int):
    assert executor.commands[command_index].cmd == "sacct"
    assert executor.commands[command_index].args[:2] == ["-j", "1234"]
