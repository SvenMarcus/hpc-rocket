from test.application.executor_testdoubles import (
    CompletedSlurmJobCommandStub, SlurmJobSubmittedCommandStub)
from test.application.launchoptions import *
from typing import Any
from unittest.mock import Mock

from hpcrocket.core.application import Application
from hpcrocket.core.environmentpreparation import CopyInstruction
from hpcrocket.core.executor import CommandExecutor, CommandExecutorFactory
from hpcrocket.core.filesystem import Filesystem, FilesystemFactory
from hpcrocket.ssh.sshexecutor import RemoteCommand


class CallOrderVerification(CommandExecutor, Filesystem):

    commands_by_executable = {
        "sbatch": SlurmJobSubmittedCommandStub,
        "sacct": CompletedSlurmJobCommandStub
    }

    def __init__(self):
        self.log = []

    def __getattr__(self, name: str) -> Any:
        return Mock(name=name)

    def exec_command(self, command: str) -> RemoteCommand:
        executable = command.split()[0]
        self.log.append(executable)
        return self.commands_by_executable[executable]()

    def close(self) -> None:
        pass

    def copy(self, source: str, target: str, overwrite: bool = False, filesystem: 'Filesystem' = None) -> None:
        self.log.append(f"copy {source} {target}")

    def delete(self, path: str) -> None:
        self.log.append(f"delete {path}")

    def exists(self, path: str) -> bool:
        return False

    def __call__(self) -> None:
        assert self.log == [
            "copy myfile.txt mycopy.txt",
            "sbatch",
            "sacct",
            "copy mycopy.txt mycollect.txt",
            "delete mycopy.txt",
        ]


class CallOrderVerificationFactory(CommandExecutorFactory, FilesystemFactory):

    def __init__(self) -> None:
        self.verifier = CallOrderVerification()

    def create_executor(self):
        return self.verifier

    def create_local_filesystem(self) -> 'Filesystem':
        return self.verifier

    def create_ssh_filesystem(self) -> 'Filesystem':
        return self.verifier


def test__given_config_with_files_to_copy_collect_and_clean__when_running__should_first_copy_to_remote_then_execute_job_then_collect_then_clean():
    options = options_with_files_to_copy_collect_and_clean(
        [CopyInstruction("myfile.txt", "mycopy.txt")],
        [CopyInstruction("mycopy.txt", "mycollect.txt")],
        ["mycopy.txt"]
    )

    factory = CallOrderVerificationFactory()
    sut = Application(factory, factory, Mock())

    sut.run(options)

    factory.verifier()
