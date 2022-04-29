import unittest
from test.application.executor_filesystem_callorder import (
    CallOrderVerification,
    VerifierReturningFilesystemFactory,
)
from test.application.launchoptions import launch_options, main_connection
from test.slurmoutput import completed_slurm_job
from test.testdoubles.executor import (
    FailedSlurmJobCommandStub,
    LoggingCommandExecutorSpy,
    SlurmJobExecutorSpy,
    SuccessfulSlurmJobCommandStub,
)
from test.testdoubles.filesystem import (
    DummyFilesystemFactory,
    MemoryFilesystemFactoryStub,
    MemoryFilesystemFake,
)
from unittest.mock import Mock

from hpcrocket.core.application import Application
from hpcrocket.core.environmentpreparation import CopyInstruction
from hpcrocket.core.executor import RunningCommand
from hpcrocket.ssh.errors import SSHError

LOCAL_FILE = "myfile.txt"
REMOTE_FILE = "mycopy.txt"
COLLECTED_FILE = "mycollect.txt"

GLOB_PATTERN = "*.txt"
NON_MATCHING_FILE = "NON_MATCHING_FILE.gif"


class ConnectionFailingCommandExecutor(LoggingCommandExecutorSpy):
    def connect(self) -> None:
        raise SSHError(main_connection().hostname)

    def close(self):
        pass

    def exec_command(self, cmd: str) -> RunningCommand:
        pass


def launch_options_with_copy():
    return launch_options(copy=[CopyInstruction(LOCAL_FILE, REMOTE_FILE)])


def launch_options_with_collect():
    return launch_options(
        collect=[CopyInstruction(REMOTE_FILE, COLLECTED_FILE)],
        watch=True,
    )


def launch_options_with_clean():
    return launch_options(clean=[REMOTE_FILE], watch=True)


def launch_options_copy_collect():
    return launch_options(
        copy=[CopyInstruction(LOCAL_FILE, REMOTE_FILE)],
        collect=[CopyInstruction(REMOTE_FILE, COLLECTED_FILE)],
        watch=True,
    )


def launch_options_copy_collect_clean():
    opts = launch_options_copy_collect()
    opts.clean_files = [REMOTE_FILE]
    return opts


def memory_fs_factory_with_default_local_file():
    local_fs = MemoryFilesystemFake([LOCAL_FILE])
    remote_fs = MemoryFilesystemFake()
    return MemoryFilesystemFactoryStub(local_fs, remote_fs)


def make_sut(executor=None, filesystem_factory=None, ui=None):
    return Application(
        executor or SlurmJobExecutorSpy(),
        filesystem_factory or DummyFilesystemFactory(),
        ui or Mock(),
    )


def make_sut_with_call_order_verification(expected_calls):
    verifier = CallOrderVerification(expected_calls)
    factory = VerifierReturningFilesystemFactory(verifier)
    sut = Application(verifier, factory, Mock())
    return sut, verifier


def assert_exists_locally(fs_factory: MemoryFilesystemFactoryStub, file: str) -> None:
    assert fs_factory.local_filesystem.exists(file)


def assert_does_not_exist_locally(
    fs_factory: MemoryFilesystemFactoryStub, file: str
) -> None:
    assert not fs_factory.local_filesystem.exists(file)


def assert_exists_on_remote(fs_factory: MemoryFilesystemFactoryStub, file: str) -> None:
    assert fs_factory.ssh_filesystem.exists(file)


def assert_does_not_exist_on_remote(
    fs_factory: MemoryFilesystemFactoryStub, file: str
) -> None:
    assert not fs_factory.ssh_filesystem.exists(file)


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

    def test__when_running_but_connection_fails__it_logs_the_error_and_exits_with_code_1(
        self,
    ):
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


class Application_With_Options_To_Copy(unittest.TestCase):
    def setUp(self) -> None:
        self.fs_factory = MemoryFilesystemFactoryStub()
        self.sut = make_sut(filesystem_factory=self.fs_factory)

    def test__when_running__copies_files_to_remote(self):
        self.fs_factory.create_local_files(LOCAL_FILE)
        options = launch_options_with_copy()

        self.sut.run(options)

        assert_exists_on_remote(self.fs_factory, REMOTE_FILE)

    def test__with_globbing__when_running__copies_only_matching_files(self):
        self.fs_factory.create_local_files(LOCAL_FILE, NON_MATCHING_FILE)

        options = launch_options(copy=[CopyInstruction(GLOB_PATTERN, "store_dir")])

        self.sut.run(options)

        assert_exists_on_remote(self.fs_factory, f"store_dir/{LOCAL_FILE}")
        assert_does_not_exist_on_remote(
            self.fs_factory, f"store_dir/{NON_MATCHING_FILE}"
        )

    def test__with_glob_copy_to_existing_path__when_running__it_rolls_back_copied_files(
        self,
    ):
        exists_on_remote = "exists_on_remote.txt"
        self.fs_factory.create_local_files(LOCAL_FILE, exists_on_remote)
        self.fs_factory.create_remote_files(exists_on_remote)

        options = launch_options(copy=[CopyInstruction(GLOB_PATTERN, "")])
        self.sut.run(options)

        assert_does_not_exist_on_remote(self.fs_factory, LOCAL_FILE)


class Application_With_Options_To_Collect(unittest.TestCase):
    def setUp(self) -> None:
        self.fs_factory = MemoryFilesystemFactoryStub()
        self.sut = make_sut(filesystem_factory=self.fs_factory)

    def test__when_running__it_collects_files_from_remote(self):
        options = launch_options_with_collect()
        self.fs_factory.create_remote_files(REMOTE_FILE)

        self.sut.run(options)

        assert_exists_locally(self.fs_factory, COLLECTED_FILE)

    def test__with_globbing__when_running__collects_only_matching_files(self):
        options = launch_options(watch=True)
        options.collect_files = [CopyInstruction("*.txt", "store_dir")]
        self.fs_factory.create_remote_files(REMOTE_FILE, NON_MATCHING_FILE)

        self.sut.run(options)

        assert_exists_locally(self.fs_factory, f"store_dir/{REMOTE_FILE}")
        assert_does_not_exist_locally(self.fs_factory, f"store_dir/{NON_MATCHING_FILE}")


class Application_With_Options_To_Clean(unittest.TestCase):
    def setUp(self) -> None:
        self.fs_factory = MemoryFilesystemFactoryStub()
        self.fs_factory.create_remote_files(REMOTE_FILE)
        self.sut = make_sut(filesystem_factory=self.fs_factory)
        self.options = launch_options_with_clean()

    def test__when_running__it_cleans_files(self):
        self.sut.run(self.options)

        assert_does_not_exist_on_remote(self.fs_factory, REMOTE_FILE)

    def test__with_globbing__when_running__cleans_only_matching_files(self):
        non_matching_file = "NON_MATCHING_FILE.gif"
        self.fs_factory.create_remote_files(REMOTE_FILE, non_matching_file)
        self.options.clean_files = ["*.txt"]

        self.sut.run(self.options)

        assert_does_not_exist_on_remote(self.fs_factory, REMOTE_FILE)
        assert_exists_on_remote(self.fs_factory, non_matching_file)


class Application_With_Options_To_Copy_Collect_Clean(unittest.TestCase):
    def setUp(self) -> None:
        self.options = launch_options_copy_collect_clean()
        self.fs_factory = memory_fs_factory_with_default_local_file()
        self.sut = make_sut(filesystem_factory=self.fs_factory)

    def test__when_running__copies_then_collects_then_cleans_files(self):
        self.sut.run(self.options)

        assert_exists_locally(self.fs_factory, COLLECTED_FILE)
        assert_does_not_exist_on_remote(self.fs_factory, REMOTE_FILE)

    def test__when_running_without_watching__it_only_copies_files(self):
        self.options.watch = False

        self.sut.run(self.options)

        assert_exists_on_remote(self.fs_factory, REMOTE_FILE)
        assert_does_not_exist_locally(self.fs_factory, COLLECTED_FILE)

    def test__when_running_without_watching__it_only_copies_and_runs_job_in_order(self):
        self.options.watch = False

        expected = ["copy myfile.txt mycopy.txt", "sbatch"]
        sut, verify = make_sut_with_call_order_verification(expected)

        sut.run(self.options)

        verify()

    def test__when_running_with_watching__it_copies_runs_job_collects_cleans_in_order(
        self,
    ):
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
