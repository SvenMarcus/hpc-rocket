import subprocess
import time
import unittest
from test.integration.pyfilesystem.test_pyfilesystembased import PyFilesystemBasedTest

import pytest
from hpcrocket.core.filesystem import Filesystem
from hpcrocket.pyfilesystem.sshfilesystem import SSHFilesystem
from hpcrocket.ssh.connectiondata import ConnectionData


@pytest.mark.integration
class TestSSHFilesystem(PyFilesystemBasedTest, unittest.TestCase):  # type: ignore
    @classmethod
    def setUpClass(cls) -> None:
        exit_code = subprocess.call(
            [
                "docker",
                "run",
                "-d",
                "--name=openssh-server",
                "-e",
                "PASSWORD_ACCESS=true",
                "-e",
                "USER_PASSWORD=1234",
                "-e",
                "USER_NAME=myuser",
                "--publish=2222:2222",
                "hpc-rocket/openssh-test-server",
            ]
        )

        time.sleep(1)
        assert exit_code == 0

    def tearDown(self) -> None:
        exit_code = subprocess.call(
            ["docker", "exec", "openssh-server", "/bin/bash", "-c", "rm -rf /testdir/*"]
        )
        assert exit_code == 0

    @classmethod
    def tearDownClass(cls) -> None:
        subprocess.call(["docker", "stop", "openssh-server"])
        subprocess.call(["docker", "rm", "openssh-server"])

    def create_filesystem(self) -> Filesystem:
        conn = ConnectionData(
            hostname="localhost", username="myuser", password="1234", port=2222
        )
        return SSHFilesystem(conn)
