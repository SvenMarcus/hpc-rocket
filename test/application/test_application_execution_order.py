from test.application.launchoptions import *
from test.testdoubles.executor import (
    CompletedSlurmJobCommandStub, SlurmJobSubmittedCommandStub)
from typing import Any
from unittest.mock import Mock

from hpcrocket.core.application import Application
from hpcrocket.core.environmentpreparation import CopyInstruction
from hpcrocket.core.executor import CommandExecutor, CommandExecutorFactory, RunningCommand
from hpcrocket.core.filesystem import Filesystem, FilesystemFactory


class CallOrderVerification(CommandExecutor, Filesystem):

    commands_by_executable = {
        "sbatch": SlurmJobSubmittedCommandStub,
        "sacct": CompletedSlurmJobCommandStub
    }

    def __init__(self, expected):
        self.log = []
        self.expected = expected

    def __getattr__(self, name: str) -> Any:
        return Mock(name=name)

    def exec_command(self, command: str) -> RunningCommand:
        executable = command.split()[0]
        self.log.append(executable)
        return self.commands_by_executable[executable]()

    def connect(self) -> None:
        pass

    def close(self) -> None:
        pass

    def copy(self, source: str, target: str, overwrite: bool = False, filesystem: 'Filesystem' = None) -> None:
        self.log.append(f"copy {source} {target}")

    def delete(self, path: str) -> None:
        self.log.append(f"delete {path}")

    def exists(self, path: str) -> bool:
        return False

    def __call__(self) -> None:
        assert self.log == self.expected


class CallOrderVerificationFactory(CommandExecutorFactory, FilesystemFactory):

    def __init__(self) -> None:
        self.verifier = None

    def create_executor(self):
        return self.verifier

    def create_local_filesystem(self) -> 'Filesystem':
        return self.verifier  # type: ignore

    def create_ssh_filesystem(self) -> 'Filesystem':
        return self.verifier  # type: ignore


def test__given_launchoptions_with_watch_disabled_files_to_copy_collect_and_clean__when_running__should_first_copy_to_remote_then_execute_job_then_exit():
    opts = options(
        copy=[CopyInstruction("myfile.txt", "mycopy.txt")],
        collect=[CopyInstruction("mycopy.txt", "mycollect.txt")],
        clean=["mycopy.txt"],
        watch=False
    )

    factory = CallOrderVerificationFactory()
    factory.verifier = CallOrderVerification([
        "copy myfile.txt mycopy.txt",
        "sbatch"
    ])

    sut = Application(factory, factory, Mock())

    sut.run(opts)

    factory.verifier()



def test__given_launchoptions_with_watch_enabled_files_to_copy_collect_and_clean__when_running__should_first_copy_to_remote_then_execute_job_then_collect_then_clean():
    opts = options(
        copy=[CopyInstruction("myfile.txt", "mycopy.txt")],
        collect=[CopyInstruction("mycopy.txt", "mycollect.txt")],
        clean=["mycopy.txt"],
        watch=True
    )

    factory = CallOrderVerificationFactory()
    factory.verifier = CallOrderVerification([
        "copy myfile.txt mycopy.txt",
        "sbatch",
        "sacct",
        "copy mycopy.txt mycollect.txt",
        "delete mycopy.txt",
    ])

    sut = Application(factory, factory, Mock())

    sut.run(opts)

    factory.verifier()