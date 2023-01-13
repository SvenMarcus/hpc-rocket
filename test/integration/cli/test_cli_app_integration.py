import os
from unittest.mock import patch

from test.slurm_assertions import assert_job_submitted
from test.testdoubles.executor import (
    LoggingCommandExecutorSpy,
    SlurmJobExecutorSpy,
    failed_slurm_job_command_stub,
    successful_slurm_job_command_stub,
)
from typing import Dict, List, Optional, cast

import fs.base
import pytest
from fs.memoryfs import MemoryFS
from hpcrocket import RuntimeContainer, ServiceRegistry
from hpcrocket.core.executor import CommandExecutor, RunningCommand
from hpcrocket.core.filesystem import Filesystem, FilesystemFactory
from hpcrocket.core.launchoptions import Options
from hpcrocket.pyfilesystem.pyfilesystembased import PyFilesystemBased
from hpcrocket.ui import RichUI

CONFIG_FILE_PATH = "test/testconfig/integration_launch_config_fail_allowed.yml"


class _TestServiceRegistry:
    def __init__(
        self, executor: CommandExecutor, fs_factory: "MemoryPyFilesystemFactory"
    ) -> None:
        self.executor = executor
        self.fs_factory = fs_factory

    def local_filesystem(self) -> Filesystem:
        return self.fs_factory.create_local_filesystem()

    def get_executor(self, options: Options) -> CommandExecutor:
        return self.executor

    def get_filesystem_factory(self, options: Options) -> FilesystemFactory:
        return self.fs_factory


def memory_fs() -> PyFilesystemBased:
    return PyFilesystemBased(MemoryFS())


class MemoryPyFilesystemFactory(FilesystemFactory):
    def __init__(self) -> None:
        self.local = memory_fs()
        self.remote = memory_fs()

    def create_local_filesystem(self) -> "Filesystem":
        return self.local

    def create_ssh_filesystem(self) -> "Filesystem":
        return self.remote


def prepare_local_filesystem(
    local_fs: fs.base.FS, *, config_file: str = CONFIG_FILE_PATH
) -> None:
    local_fs.makedirs("/home/myuser/.ssh")
    local_fs.create("/home/myuser/.ssh/id_ed25519")
    local_fs.create("/home/myuser/.ssh/proxy_key")

    local_fs.makedirs("/home/myuser/dir/subdir")
    local_fs.writetext("/home/myuser/dir/test.txt", "testfile")
    local_fs.writetext("/home/myuser/dir/hello.txt", "hellofile")
    local_fs.writetext("/home/myuser/dir/subdir/next.txt", "hello next")
    local_fs.create("local_slurm.job")

    with open(config_file, "r") as file:
        local_fs.writetext("config.yml", file.read())


def prepare_environment_variables() -> Dict[str, str]:
    return {
        "ABS_DIR": "/home/myuser",
        "HOME": "/home/myuser",
        "TARGET_USER": "target_user",
        "TARGET_HOST": "target_host.example.com",
        "TARGET_KEY": "~/.ssh/id_ed25519",
        "PROXY_USER": "proxy_user",
        "PROXY_HOST": "proxy_host.server.com",
        "PROXY_KEY": "~/.ssh/proxy_key",
        "REMOTE_SLURM_SCRIPT": "my_slurm_job.job",
    }


@pytest.mark.integration
@patch.dict(os.environ, prepare_environment_variables())
def test__when_running_launch__it_connects_to_remote_and_launches_job_with_executor() -> None:
    registry = create_service_registry()
    fs_factory = registry.fs_factory
    executor = cast(LoggingCommandExecutorSpy, registry.executor)

    prepare_environment_variables()
    prepare_local_filesystem(fs_factory.local.internal_fs)

    args = ["hpc-rocket", "launch", "config.yml"]
    exit_code = run_with_args(registry, args)

    assert_job_submitted(executor, "my_slurm_job.job")
    assert exit_code == 0


@pytest.mark.integration
@pytest.mark.parametrize(
    "job_result_command",
    (successful_slurm_job_command_stub(), failed_slurm_job_command_stub()),
)
@patch.dict(os.environ, prepare_environment_variables())
def test__when_running_launch_with_watching_and_fail_allowed__it_copies_runs_job_collects_and_cleans_files(
    job_result_command: RunningCommand,
) -> None:
    registry = create_service_registry(job_result_command)
    fs_factory = registry.fs_factory

    prepare_local_filesystem(fs_factory.local.internal_fs)

    args = ["hpc-rocket", "launch", "--watch", "config.yml"]
    run_with_args(registry, args)

    print("\n")
    fs_factory.remote.internal_fs.tree()
    assert fs_factory.local.exists("test.txt")
    assert fs_factory.local.exists("hello.txt")
    assert fs_factory.remote.exists("target/hello.txt")
    assert fs_factory.remote.exists("target/test.txt")
    assert fs_factory.remote.exists("target/subdir/next.txt")
    assert not fs_factory.remote.exists("my_slurm_job.job")


def create_service_registry(
    job_result_command: Optional[RunningCommand] = None,
) -> _TestServiceRegistry:
    executor = SlurmJobExecutorSpy(job_result_command)
    return _TestServiceRegistry(executor, MemoryPyFilesystemFactory())


def run_with_args(registry: ServiceRegistry, args: List[str]) -> int:
    with RichUI() as ui:
        sut = RuntimeContainer(args, registry, ui)
        exit_code = sut.run()
        return exit_code
