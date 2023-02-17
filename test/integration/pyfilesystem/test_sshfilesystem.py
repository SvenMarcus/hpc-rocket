import time
import unittest
from test.integration.pyfilesystem.test_pyfilesystembased import PyFilesystemBasedTest

import pytest
from testcontainers.core.container import DockerContainer
from hpcrocket.core.filesystem import Filesystem
from hpcrocket.pyfilesystem.sshfilesystem import sshfilesystem
from hpcrocket.ssh.connectiondata import ConnectionData


def configure_container() -> DockerContainer:
    return (
        DockerContainer(image="lscr.io/linuxserver/openssh-server:latest")
        .with_bind_ports(2222, 2222)
        .with_env("PASSWORD_ACCESS", "true")
        .with_env("USER_PASSWORD", "1234")
        .with_env("USER_NAME", "testcontainer")
    )


@pytest.mark.integration
class TestSSHFilesystem(PyFilesystemBasedTest, unittest.TestCase):
    CONTAINER: DockerContainer

    @classmethod
    def setUpClass(cls) -> None:
        cls.CONTAINER = configure_container()
        cls.CONTAINER.start()
        time.sleep(1)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.CONTAINER.stop()

    def setUp(self) -> None:
        TestSSHFilesystem.CONTAINER.exec("mkdir /testdir")
        TestSSHFilesystem.CONTAINER.exec("usermod -d /testdir testcontainer")
        TestSSHFilesystem.CONTAINER.exec(
            "chown 'testcontainer':'testcontainer' /testdir"
        )

    def tearDown(self) -> None:
        TestSSHFilesystem.CONTAINER.exec("rm -rf /testdir")

    def create_filesystem(self, dir: str = "/testdir") -> Filesystem:
        conn = ConnectionData(
            hostname="localhost", username="testcontainer", password="1234", port=2222
        )
        return sshfilesystem(conn, dir=dir)

    def working_dir_abs(self) -> str:
        return "/testdir"

    def home_dir_abs(self) -> str:
        return "/testdir"
