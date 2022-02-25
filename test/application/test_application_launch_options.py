import unittest
from test.application.executor_filesystem_callorder import (CallOrderVerification,
                                                            VerifierReturningFilesystemFactory)
from test.application.launchoptions import launch_options, main_connection
from test.slurmoutput import completed_slurm_job
from test.testdoubles.executor import (FailedSlurmJobCommandStub,
                                       LoggingCommandExecutorSpy,
                                       SlurmJobExecutorSpy,
                                       SuccessfulSlurmJobCommandStub)
from test.testdoubles.filesystem import (DummyFilesystemFactory,
                                         MemoryFilesystemFactoryStub,
                                         MemoryFilesystemFake)
from unittest.mock import Mock

from hpcrocket.core.application import Application
from hpcrocket.core.environmentpreparation import CopyInstruction
from hpcrocket.core.executor import RunningCommand
from hpcrocket.ssh.errors import SSHError

DEFAULT_LOCAL_FILE = "myfile.txt"
DEFAULT_REMOTE_FILE = "mycopy.txt"
DEFAULT_LOCAL_COLLECTED = "mycollect.txt"


class ConnectionFailingCommandExecutor(LoggingCommandExecutorSpy):

    def connect(self) -> None:
        raise SSHError(main_connection().hostname)

    def close(self):
        pass

    def exec_command(self, cmd: str) -> RunningCommand:
        pass


def launch_options_copy_collect():
    return launch_options(
        copy=[CopyInstruction(DEFAULT_LOCAL_FILE, DEFAULT_REMOTE_FILE)],
        collect=[CopyInstruction(
            DEFAULT_REMOTE_FILE,
            DEFAULT_LOCAL_COLLECTED
        )],
        watch=True
    )


def launch_options_copy_collect_clean():
    opts = launch_options_copy_collect()
    opts.clean_files = [DEFAULT_REMOTE_FILE]
    return opts


def memory_fs_factory_with_default_local_file():
    local_fs = MemoryFilesystemFake([DEFAULT_LOCAL_FILE])
    remote_fs = MemoryFilesystemFake()
    return MemoryFilesystemFactoryStub(local_fs, remote_fs)


def make_sut(executor=None, filesystem_factory=None, ui=None):
    return Application(
        executor or SlurmJobExecutorSpy(),
        filesystem_factory or DummyFilesystemFactory(),
        ui or Mock()
    )


def make_sut_with_call_order_verification(expected_calls):
    verifier = CallOrderVerification(expected_calls)
    factory = VerifierReturningFilesystemFactory(verifier)
    sut = Application(verifier, factory, Mock())
    return sut, verifier


class Application_With_Launch_Options(unittest.TestCase):

    def setUp(self) -> None:
        self.executor = SlurmJobExecutorSpy()
        self.ui_spy = Mock()
        self.sut = make_sut(self.executor, ui=self.ui_spy)

    def test__when_running__it_runs_sbatch_with_executor(self):
        self.sut.run(launch_options())

        actual_sbatch = str(self.executor.command_log[0])
        assert actual_sbatch == f"sbatch {launch_options().sbatch}"

    def test__when_sbatch_job_succeeds__should_return_exit_code_zero(self):
        self.executor.sacct_cmd = SuccessfulSlurmJobCommandStub()
        actual = self.sut.run(launch_options(watch=True))

        assert actual == 0

    def test__when_sbatch_job_fails__should_return_exit_code_one(self):
        self.executor.sacct_cmd = FailedSlurmJobCommandStub()

        actual = self.sut.run(launch_options(watch=True))

        assert actual == 1

    def test__when_running__it_updates_ui_with_job_state_after_polling(self):
        _ = self.sut.run(launch_options(watch=True))

        self.ui_spy.update.assert_called_with(completed_slurm_job())

    def test__when_running_but_connection_fails__it_logs_the_error_and_exits_with_code_1(self):
        self.executor = ConnectionFailingCommandExecutor()
        self.sut = make_sut(self.executor, ui=self.ui_spy)

        actual = self.sut.run(launch_options(watch=True))

        self.assert_error_logged(f"SSHError: {main_connection().hostname}")
        self.assert_exited_without_running_commands(actual)

    def assert_error_logged(self, expected_message):
        self.ui_spy.error.assert_called_once_with(expected_message)

    def assert_exited_without_running_commands(self, actual):
        assert self.executor.command_log == []
        assert actual == 1


class Application_With_Options_To_Copy_And_Collect(unittest.TestCase):

    def test__when_running__copies_and_collects_files(self):
        opts = launch_options_copy_collect()

        fs_factory = memory_fs_factory_with_default_local_file()
        sut = make_sut(filesystem_factory=fs_factory)

        sut.run(opts)

        local_fs = fs_factory.local_filesystem
        remote_fs = fs_factory.ssh_filesystem
        assert local_fs.exists(DEFAULT_LOCAL_COLLECTED)
        assert remote_fs.exists(DEFAULT_REMOTE_FILE)


class Application_With_Options_To_Copy_Collect_Clean(unittest.TestCase):

    def setUp(self) -> None:
        self.options = launch_options_copy_collect_clean()

    def test__when_running_without_watching__it_only_copies_files(self):
        self.options.watch = False

        fs_factory = memory_fs_factory_with_default_local_file()
        sut = make_sut(filesystem_factory=fs_factory)

        sut.run(self.options)

        local_fs = fs_factory.local_filesystem
        remote_fs = fs_factory.ssh_filesystem
        assert remote_fs.exists(DEFAULT_REMOTE_FILE)
        assert local_fs.exists(DEFAULT_LOCAL_COLLECTED) is False

    def test__when_running_without_watching__it_only_copies_and_runs_job_in_order(self):
        self.options.watch = False

        expected = [
            "copy myfile.txt mycopy.txt",
            "sbatch"
        ]

        sut, verify = make_sut_with_call_order_verification(expected)

        sut.run(self.options)

        verify()

    def test__when_running_with_watching__it_copies_runs_job_collects_cleans_in_order(self):
        expected = [
            "copy myfile.txt mycopy.txt",
            "sbatch",
            "sacct",
            "copy mycopy.txt mycollect.txt",
            "delete mycopy.txt",
        ]

        sut, verify = make_sut_with_call_order_verification(expected)

        sut.run(self.options)

        verify()
