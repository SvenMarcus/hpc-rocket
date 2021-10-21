import pytest

from unittest.mock import DEFAULT, Mock, create_autospec

from hpcrocket.core.environmentpreparation import CopyInstruction
from hpcrocket.core.filesystem import Filesystem
from hpcrocket.core.slurmcontroller import SlurmController
from test.application.launchoptions import options
from test.testdoubles.executor import (FailedSlurmJobCommandStub,
                                       LongRunningSlurmJobExecutorSpy,
                                       SlurmJobExecutorSpy,
                                       SuccessfulSlurmJobCommandStub)
from test.slurm_assertions import assert_job_submitted, assert_job_polled, assert_job_canceled
from test.testdoubles.filesystem import DummyFilesystemFactory, MemoryFilesystem, MemoryFilesystemFactoryStub

from hpcrocket.core.workflows import LaunchWorkflow


@pytest.fixture
def executor_spy():
    return SlurmJobExecutorSpy()


def test__given_simple_launchoptions__when_running__should_run_sbatch_with_executor(executor_spy):
    opts = options()
    sut = LaunchWorkflow(DummyFilesystemFactory(), opts)

    controller = SlurmController(executor_spy)
    sut.run(controller)

    assert_job_submitted(executor_spy, opts.sbatch)


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


def test__given_options_without_watching__when_running__should_only_sbatch_then_exit(executor_spy):
    opts = options(watch=False)
    sut = LaunchWorkflow(DummyFilesystemFactory(), opts)

    controller = SlurmController(executor_spy)
    sut.run(controller)

    assert_job_submitted(executor_spy, opts.sbatch)
    assert len(executor_spy.command_log) == 1


def test__given_options_with_files_to_copy__when_running__should_copy_files_to_remote_with_given_overwrite_settings(
        executor_spy):
    opts = options(copy=[CopyInstruction("myfile.txt", "mycopy.txt", True)])

    local_fs_mock = create_autospec(spec=Filesystem)
    factory = MemoryFilesystemFactoryStub(local_fs=local_fs_mock)

    sut = LaunchWorkflow(factory, opts)
    controller = SlurmController(executor_spy)

    sut.run(controller)

    local_fs_mock.copy.assert_called_with(
        source="myfile.txt",
        target="mycopy.txt",
        overwrite=True,
        filesystem=factory.ssh_filesystem)


@pytest.mark.parametrize("error_type", (FileNotFoundError, FileExistsError))
def test__given_options_with_files_to_copy__when_raising_error_during_run__should_rollback_copied_files(
        executor_spy, error_type):
    opts = options(copy=[
        CopyInstruction("myfile.txt", "mycopy.txt"),
        CopyInstruction("myfile.txt", "mycopy.txt")
    ])

    local_fs = Mock(wraps=MemoryFilesystem(files=["myfile.txt"]))
    local_fs.copy.side_effect = raise_on_second_call(error_type)
    factory = MemoryFilesystemFactoryStub(local_fs)

    sut = LaunchWorkflow(factory, opts)

    controller = SlurmController(executor_spy)
    sut.run(controller)

    assert factory.ssh_filesystem.exists("mycopy.txt") == False


def test__given_options_with_files_to_copy__when_running_with_error_during_copy__should_immediately_exit_with_1(
        executor_spy):

    opts = options(copy=[CopyInstruction("myfile.txt", "mycopy.txt")])

    local_fs = Mock(spec=Filesystem)
    local_fs.copy.side_effect = FileNotFoundError
    factory = MemoryFilesystemFactoryStub(local_fs)

    sut = LaunchWorkflow(factory, opts)

    controller = SlurmController(executor_spy)
    actual = sut.run(controller)

    assert len(executor_spy.command_log) == 0
    assert actual == 1


def raise_on_second_call(error):
    calls = 0

    def raise_on_second_call(*args, **kwargs):
        nonlocal calls
        calls += 1

        if calls == 2:
            raise error

        return DEFAULT

    return raise_on_second_call
