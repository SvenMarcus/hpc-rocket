import os
import unittest
from pathlib import Path
from test.application import make_application
from test.application.assertions import (
    assert_does_not_exist_locally,
    assert_does_not_exist_on_remote,
    assert_exists_locally,
    assert_exists_on_remote,
)
from test.application.executor_filesystem_callorder import (
    CallOrderVerification,
    VerifierReturningFilesystemFactory,
)
from test.application.optionbuilders import (
    COLLECTED_FILE,
    GLOB_PATTERN,
    LOCAL_DIR,
    LOCAL_FILE,
    NON_MATCHING_FILE,
    REMOTE_DIR,
    REMOTE_FILE,
    launch_options,
    launch_options_copy_collect_clean,
    launch_options_with_copy,
    main_connection,
)
from test.slurmoutput import DEFAULT_JOB_ID, completed_slurm_job
from test.testdoubles.executor import (
    LoggingCommandExecutorSpy,
    SlurmJobExecutorSpy,
    failed_slurm_job_command_stub,
    successful_slurm_job_command_stub,
)
from test.testdoubles.filesystem import (
    MemoryFilesystemFactoryStub,
    MemoryFilesystemFake,
)
from typing import List, Tuple
from unittest.mock import Mock

from hpcrocket.core.application import Application
from hpcrocket.core.executor import RunningCommand
from hpcrocket.core.filesystem.progressive import CopyInstruction
from hpcrocket.ssh.errors import SSHError


class ConnectionFailingCommandExecutor(LoggingCommandExecutorSpy):
    def connect(self) -> None:
        raise SSHError(main_connection().hostname)

    def close(self) -> None:
        raise NotImplementedError()

    def exec_command(self, cmd: str) -> RunningCommand:
        raise NotImplementedError()


def memory_fs_factory_with_default_local_file() -> MemoryFilesystemFactoryStub:
    local_fs = MemoryFilesystemFake([LOCAL_FILE])
    remote_fs = MemoryFilesystemFake()
    return MemoryFilesystemFactoryStub(local_fs, remote_fs)


def make_sut_with_call_order_verification(
    expected_calls: List[str],
) -> Tuple[Application, CallOrderVerification]:
    verifier = CallOrderVerification(expected_calls)
    factory = VerifierReturningFilesystemFactory(verifier)
    sut = Application(verifier, factory, Mock())
    return sut, verifier


class Application_With_Launch_Options(unittest.TestCase):

    LOG_FILE = "test.log"

    def setUp(self) -> None:
        self.executor = SlurmJobExecutorSpy()
        self.ui_spy = Mock()
        self.sut = make_application(self.executor, ui=self.ui_spy)

    def tearDown(self) -> None:
        Path(self.LOG_FILE).unlink(missing_ok=True)

    def test__when_running__it_runs_sbatch_with_executor(self) -> None:
        self.sut.run(launch_options())

        actual_sbatch = str(self.executor.command_log[0])
        assert actual_sbatch == f"sbatch {launch_options().job}"

    def test__when_sbatch_job_succeeds__should_return_exit_code_zero(self) -> None:
        self.executor.sacct_cmd = successful_slurm_job_command_stub()
        actual = self.sut.run(launch_options(watch=True))

        assert actual == 0

    def test__when_sbatch_job_fails__should_return_exit_code_one(self) -> None:
        self.executor.sacct_cmd = failed_slurm_job_command_stub()

        actual = self.sut.run(launch_options(watch=True))

        assert actual == 1

    def test__when_running__it_updates_ui_with_job_state_after_polling(self) -> None:
        _ = self.sut.run(launch_options(watch=True))

        self.ui_spy.update.assert_called_with(completed_slurm_job())

    def test__when_running_but_connection_fails__it_logs_the_error_and_exits_with_code_1(
        self,
    ) -> None:
        executor = ConnectionFailingCommandExecutor()
        self.sut = make_application(executor, ui=self.ui_spy)

        actual = self.sut.run(launch_options(watch=True))

        self.assert_error_logged(f"SSHError: {main_connection().hostname}")
        self.assert_exited_without_running_commands(actual)

    def test__when_adding_log_job_id__stores_job_id_in_logfile(self) -> None:
        options = launch_options()
        options.job_id_file = self.LOG_FILE

        self.sut.run(options)

        Path(self.LOG_FILE).read_text() == DEFAULT_JOB_ID

    def assert_error_logged(self, expected_message: str) -> None:
        self.ui_spy.error.assert_called_once_with(expected_message)

    def assert_exited_without_running_commands(self, actual: int) -> None:
        assert self.executor.command_log == []
        assert actual == 1


class Application_With_Options_To_Copy(unittest.TestCase):
    def setUp(self) -> None:
        self.fs_factory = MemoryFilesystemFactoryStub()
        self.sut = make_application(filesystem_factory=self.fs_factory)

    def test__when_running__copies_files_to_remote(self) -> None:
        self.fs_factory.create_local_files(LOCAL_FILE)
        options = launch_options_with_copy()

        self.sut.run(options)

        assert_exists_on_remote(self.fs_factory, REMOTE_FILE)

    def test__file_into_dir__when_running__copies_file_into_remote_dir(self) -> None:
        self.fs_factory.create_local_files(LOCAL_FILE)
        options = launch_options()
        options.copy_files = [CopyInstruction(LOCAL_FILE, REMOTE_DIR)]

        self.sut.run(options)

        expected_path = os.path.join(REMOTE_DIR, LOCAL_FILE)
        assert_exists_on_remote(self.fs_factory, expected_path)

    def test__with_globbing__when_running__copies_only_matching_files(self) -> None:
        self.fs_factory.create_local_files(LOCAL_FILE, NON_MATCHING_FILE)

        options = launch_options(copy=[CopyInstruction(GLOB_PATTERN, REMOTE_DIR)])

        self.sut.run(options)

        expected_path = os.path.join(REMOTE_DIR, LOCAL_FILE)
        invalid_path = os.path.join(REMOTE_DIR, NON_MATCHING_FILE)
        assert_exists_on_remote(self.fs_factory, expected_path)
        assert_does_not_exist_on_remote(self.fs_factory, invalid_path)

    def test__with_globbing_into_nested_dir__when_running_copies_files_into_target_dir(
        self,
    ) -> None:
        nested_file = os.path.join(LOCAL_DIR, LOCAL_FILE)
        nested_glob = os.path.join(LOCAL_DIR, GLOB_PATTERN)
        self.fs_factory.create_local_files(nested_file)

        options = launch_options(copy=[CopyInstruction(nested_glob, REMOTE_DIR)])

        self.sut.run(options)

        expected_path = os.path.join(REMOTE_DIR, LOCAL_FILE)
        assert_exists_on_remote(self.fs_factory, expected_path)

    def test__with_glob_copy_to_existing_path__when_running__it_rolls_back_copied_files(
        self,
    ) -> None:
        exists_on_remote = "exists_on_remote.txt"
        self.fs_factory.create_local_files(LOCAL_FILE, exists_on_remote)
        self.fs_factory.create_remote_files(exists_on_remote)

        options = launch_options(copy=[CopyInstruction(GLOB_PATTERN, "")])
        self.sut.run(options)

        assert_does_not_exist_on_remote(self.fs_factory, LOCAL_FILE)

    def test__with_trailing_glob__when_running__copies_entire_subtree(
        self,
    ) -> None:
        self.fs_factory.create_local_files("dir/first.txt", "dir/subdir/second.txt")
        options = launch_options(copy=[CopyInstruction("dir/*", "target")])

        self.sut.run(options)

        assert_exists_on_remote(self.fs_factory, "target/first.txt")
        assert_exists_on_remote(self.fs_factory, "target/subdir/second.txt")


class Application_With_Options_To_Copy_Collect_Clean(unittest.TestCase):
    def setUp(self) -> None:
        self.options = launch_options_copy_collect_clean()
        self.fs_factory = memory_fs_factory_with_default_local_file()
        self.sut = make_application(filesystem_factory=self.fs_factory)

    def test__when_running__copies_then_collects_then_cleans_files(self) -> None:
        self.sut.run(self.options)

        assert_exists_locally(self.fs_factory, COLLECTED_FILE)
        assert_does_not_exist_on_remote(self.fs_factory, REMOTE_FILE)

    def test__when_running_without_watching__it_only_copies_files(self) -> None:
        self.options.watch = False

        self.sut.run(self.options)

        assert_exists_on_remote(self.fs_factory, REMOTE_FILE)
        assert_does_not_exist_locally(self.fs_factory, COLLECTED_FILE)

    def test__when_running_without_watching__it_only_copies_and_runs_job_in_order(
        self,
    ) -> None:
        self.options.watch = False

        expected = ["copy myfile.txt mycopy.txt", "sbatch"]
        sut, verify = make_sut_with_call_order_verification(expected)

        sut.run(self.options)

        verify()

    def test__when_running_with_watching__it_copies_runs_job_collects_cleans_in_order(
        self,
    ) -> None:
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

    def test__whe_job_fails__but_allowed_to_fail__collects_and_cleans(self) -> None:
        self.options.continue_if_job_fails = True
        executor = SlurmJobExecutorSpy(sacct_cmd=failed_slurm_job_command_stub())
        self.sut = make_application(
            executor=executor, filesystem_factory=self.fs_factory
        )

        self.sut.run(self.options)

        assert_exists_locally(self.fs_factory, "mycollect.txt")
        assert_does_not_exist_on_remote(self.fs_factory, "mycopy.txt")

    def test__job_fails_but_allowed_to_run_cleanup__exits_with_error_code(self) -> None:
        self.options.continue_if_job_fails = True
        executor = SlurmJobExecutorSpy(sacct_cmd=failed_slurm_job_command_stub())
        self.sut = make_application(
            executor=executor, filesystem_factory=self.fs_factory
        )

        actual = self.sut.run(self.options)

        assert actual == 1
