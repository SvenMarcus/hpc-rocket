import os
from test.slurm_assertions import assert_job_submitted
from test.testdoubles.executor import LoggingCommandExecutorSpy, SlurmJobExecutorSpy
from typing import List, cast

import fs.base
import pytest
from fs.memoryfs import MemoryFS
from hpcrocket import RuntimeContainer, ServiceRegistry
from hpcrocket.core.executor import CommandExecutor
from hpcrocket.core.filesystem import Filesystem, FilesystemFactory
from hpcrocket.core.launchoptions import Options
from hpcrocket.pyfilesystem.pyfilesystembased import PyFilesystemBased
from hpcrocket.ui import RichUI


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


class MemoryPyFilesystem(PyFilesystemBased):
    def __init__(self) -> None:
        super().__init__()
        self._internal_fs = MemoryFS()

    @property
    def internal_fs(self) -> fs.base.FS:
        return self._internal_fs


class MemoryPyFilesystemFactory(FilesystemFactory):
    def __init__(self) -> None:
        self.local = MemoryPyFilesystem()
        self.remote = MemoryPyFilesystem()

    def create_local_filesystem(self) -> "Filesystem":
        return self.local

    def create_ssh_filesystem(self) -> "Filesystem":
        return self.remote


@pytest.mark.integration
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
def test__when_running_launch_with_watching__it_copies_runs_job_collects_and_cleans_files() -> None:
    registry = create_service_registry()
    fs_factory = registry.fs_factory

    prepare_environment_variables()
    prepare_local_filesystem(fs_factory.local.internal_fs)

    args = ["hpc-rocket", "launch", "--watch", "config.yml"]
    run_with_args(registry, args)

    assert fs_factory.local.exists("test.txt")
    assert fs_factory.local.exists("hello.txt")
    assert not fs_factory.remote.exists("my_slurm_job.job")


def create_service_registry() -> _TestServiceRegistry:
    registry = _TestServiceRegistry(SlurmJobExecutorSpy(), MemoryPyFilesystemFactory())
    return registry


def run_with_args(registry: ServiceRegistry, args: List[str]) -> int:
    with RichUI() as ui:
        sut = RuntimeContainer(args, registry, ui)
        exit_code = sut.run()
        return exit_code


def prepare_local_filesystem(local_fs: fs.base.FS) -> None:
    local_fs.makedirs("/home/myuser/.ssh")
    local_fs.create("/home/myuser/.ssh/id_ed25519")
    local_fs.create("/home/myuser/.ssh/proxy_key")

    local_fs.makedirs("localdir")
    local_fs.writetext("localdir/test.txt", "testfile")
    local_fs.writetext("localdir/hello.txt", "hellofile")
    local_fs.create("local_slurm.job")

    with open("test/testconfig/integration_launch_config.yml", "r") as file:
        local_fs.writetext("config.yml", file.read())


def prepare_environment_variables() -> None:
    os.environ["HOME"] = "/home/myuser"
    os.environ["TARGET_USER"] = "target_user"
    os.environ["TARGET_HOST"] = "target_host.example.com"
    os.environ["TARGET_KEY"] = "~/.ssh/id_ed25519"
    os.environ["PROXY_USER"] = "proxy_user"
    os.environ["PROXY_HOST"] = "proxy_host.server.com"
    os.environ["PROXY_KEY"] = "~/.ssh/proxy_key"
    os.environ["REMOTE_SLURM_SCRIPT"] = "my_slurm_job.job"
